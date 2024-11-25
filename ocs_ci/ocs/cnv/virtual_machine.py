"""
Virtual machine class
"""

import yaml
import logging

from ocs_ci.helpers.cnv_helpers import (
    create_pvc_using_data_source,
    create_volume_import_source,
    create_vm_secret,
    create_dv,
    create_role,
)

from ocs_ci.helpers.helpers import (
    create_unique_resource_name,
    create_project,
    wait_for_resource_state,
    create_resource,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.cnv.virtctl import Virtctl
from ocs_ci.ocs.cnv.virtual_machine_instance import VirtualMachineInstance
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pvc
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.exceptions import UsernameNotFoundException, CommandFailed
from ocs_ci.helpers import cnv_helpers


logger = logging.getLogger(__name__)


class VirtualMachine(Virtctl):
    """
    Virtual Machine class which provides VM information and handles various VM related operations
    like create / start / stop / status / restart/ etc
    """

    def __init__(
        self,
        vm_name,
        namespace=None,
    ):
        """
        Initialize the VirtualMachine object.

        Args:
            vm_name (str): Name of the VirtualMachine.
            namespace (str): Namespace for the VirtualMachine.

        """
        self._vm_name = vm_name
        self.namespace = (
            namespace if namespace else create_unique_resource_name("vm", "namespace")
        )
        super().__init__(namespace=self.namespace)
        self.ns_obj = None
        self.pvc_obj = None
        self.dv_obj = None
        self.source_pvc = ""
        self.sc_name = ""
        self.source_pvc_size = ""
        self.source_pvc_access_mode = ""
        self.dv_cr_data_obj = self.dv_rb_data_obj = None
        self.secret_obj = None
        self.volumeimportsource_obj = None
        self.volume_interface = ""
        self.vm_ocp_obj = OCP(
            kind=constants.VIRTUAL_MACHINE,
            namespace=self.namespace,
        )
        self.vmi_ocp_obj = OCP(
            kind=constants.VIRTUAL_MACHINE_INSTANCE,
            namespace=self.namespace,
        )
        self.vmi_obj = VirtualMachineInstance(
            vmi_name=self._vm_name, namespace=self.namespace
        )

    @property
    def name(self):
        return self._vm_name

    def create_vm_workload(
        self,
        volume_interface=constants.VM_VOLUME_PVC,
        sc_name=constants.DEFAULT_CNV_CEPH_RBD_SC,
        access_mode=constants.ACCESS_MODE_RWX,
        pvc_size="30Gi",
        source_url=constants.CNV_CENTOS_SOURCE,
        ssh=True,
        verify=True,
        vm_dict_path=None,
    ):
        """
        Create a Virtual Machine (VM) in the specified namespace using a standalone Persistent Volume Claim (PVC)

        Args:
            volume_interface (str): The type of volume interface to use. Default is `constants.VM_VOLUME_PVC`.
            ssh (bool): If set to True, it adds a statically manged public SSH key during the VM creation
            verify (bool): Set to True for to verify vm is running and ssh connectivity, False otherwise
            vm_dict_path (str): Path to the VM YAML file
            access_mode (str): The access mode for the volume. Default is `constants.ACCESS_MODE_RWX`
            sc_name (str): The name of the storage class to use. Default is `constants.DEFAULT_CNV_CEPH_RBD_SC`.
            pvc_size (str): The size of the PVC. Default is "30Gi".
            source_url (str): The URL of the vm registry image. Default is `constants.CNV_CENTOS_SOURCE`.

        Returns:
            vm_obj: The VirtualMachine object

        Raises:
            CommandFailed: If an error occurs during the creation of the VM

        """
        self.volume_interface = volume_interface
        self.sc_name = sc_name
        self.source_pvc_size = pvc_size
        self.source_pvc_access_mode = access_mode
        # Create namespace if it doesn't exist
        try:
            self.ns_obj = create_project(project_name=self.namespace)
        except CommandFailed as ex:
            if "(AlreadyExists)" in str(ex):
                logger.warning(f"The namespace: {self.namespace} already exists!")
        vm_dict_path = vm_dict_path if vm_dict_path else constants.CNV_VM_TEMPLATE_YAML
        vm_data = templating.load_yaml(vm_dict_path)
        vm_data["metadata"]["name"] = self._vm_name
        vm_data["metadata"]["namespace"] = self.namespace
        if ssh:
            self.secret_obj = create_vm_secret(namespace=self.namespace)
            ssh_secret_dict = [
                {
                    "sshPublicKey": {
                        "propagationMethod": {"noCloud": {}},
                        "source": {"secret": {"secretName": f"{self.secret_obj.name}"}},
                    }
                }
            ]
            vm_data["spec"]["template"]["spec"]["accessCredentials"] = ssh_secret_dict

        if volume_interface == constants.VM_VOLUME_PVC:
            self.volumeimportsource_obj = create_volume_import_source(url=source_url)
            self.pvc_obj = create_pvc_using_data_source(
                source_name=self.volumeimportsource_obj.name,
                pvc_size=pvc_size,
                sc_name=sc_name,
                access_mode=access_mode,
                namespace=self.namespace,
            )
            vm_data["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"][
                "claimName"
            ] = self.pvc_obj.name
            self.source_pvc = self.pvc_obj.name
            wait_for_resource_state(
                resource=self.pvc_obj, state=constants.STATUS_BOUND, timeout=300
            )
        if volume_interface == constants.VM_VOLUME_DV:
            self.dv_obj = create_dv(
                pvc_size=pvc_size,
                sc_name=sc_name,
                access_mode=access_mode,
                namespace=self.namespace,
                source_url=source_url,
            )
            self.source_pvc = self.dv_obj.name
            del vm_data["spec"]["template"]["spec"]["volumes"][0][
                "persistentVolumeClaim"
            ]
            vm_data["spec"]["template"]["spec"]["volumes"][0]["dataVolume"] = {
                "name": f"{self.dv_obj.name}"
            }

        if volume_interface == constants.VM_VOLUME_DVT:
            # Define the dataVolumeTemplates content with parameters
            dvt_name = create_unique_resource_name("test", "dvt")
            vm_data["spec"]["dataVolumeTemplates"] = []
            metadata = {
                "name": dvt_name,
                "annotations": {"cdi.kubevirt.io/storage.checkStaticVolume": "true"},
            }
            storage_spec = {
                "storage": {
                    "accessModes": [access_mode],
                    "storageClassName": sc_name,
                    "resources": {"requests": {"storage": pvc_size}},
                },
                "source": {"registry": {"url": source_url}},
            }

            vm_data["spec"]["dataVolumeTemplates"].append(
                {"metadata": metadata, "spec": storage_spec}
            )
            self.source_pvc = dvt_name
            del vm_data["spec"]["template"]["spec"]["volumes"][0][
                "persistentVolumeClaim"
            ]
            vm_data["spec"]["template"]["spec"]["volumes"][0]["dataVolume"] = {
                "name": f"{dvt_name}"
            }

        vm_ocs_obj = create_resource(**vm_data)
        logger.info(f"Successfully created VM: {vm_ocs_obj.name}")

        if verify:
            self.verify_vm(verify_ssh=ssh)

    def clone_vm(
        self,
        source_vm_obj,
        volume_interface,
        ssh=True,
        verify=True,
    ):

        self.volume_interface = source_vm_obj.volume_interface
        self.sc_name = source_vm_obj.sc_name
        self.source_pvc_size = source_vm_obj.source_pvc_size
        self.source_pvc_access_mode = source_vm_obj.source_pvc_access_mode
        # Create namespace if it doesn't exist
        try:
            self.ns_obj = create_project(project_name=self.namespace)
        except CommandFailed as ex:
            if "(AlreadyExists)" in str(ex):
                logger.warning(f"The namespace: {self.namespace} already exists!")
        vm_data = templating.load_yaml(constants.CNV_VM_TEMPLATE_YAML)
        vm_data["metadata"]["name"] = self._vm_name
        vm_data["metadata"]["namespace"] = self.namespace
        if ssh:
            self.secret_obj = create_vm_secret(namespace=self.namespace)
            ssh_secret_dict = [
                {
                    "sshPublicKey": {
                        "propagationMethod": {"noCloud": {}},
                        "source": {"secret": {"secretName": f"{self.secret_obj.name}"}},
                    }
                }
            ]
            vm_data["spec"]["template"]["spec"]["accessCredentials"] = ssh_secret_dict

        if volume_interface == constants.VM_VOLUME_PVC:
            self.pvc_obj = pvc.create_pvc_clone(
                sc_name=self.sc_name,
                parent_pvc=source_vm_obj.source_pvc,
                clone_yaml=constants.CSI_RBD_PVC_CLONE_YAML,
                namespace=self.namespace,
                storage_size=self.source_pvc_size,
                access_mode=self.source_pvc_access_mode,
                volume_mode=constants.VOLUME_MODE_BLOCK,
            )
            vm_data["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"][
                "claimName"
            ] = self.pvc_obj.name

        if volume_interface == constants.VM_VOLUME_DV:
            self.dv_obj = create_dv(
                source_pvc_name=source_vm_obj.source_pvc,
                source_pvc_ns=source_vm_obj.namespace,
                namespace=self.namespace,
            )
            del vm_data["spec"]["template"]["spec"]["volumes"][0][
                "persistentVolumeClaim"
            ]
            vm_data["spec"]["template"]["spec"]["volumes"][0]["dataVolume"] = {
                "name": self.dv_obj.name
            }

        if volume_interface == constants.VM_VOLUME_DVT:
            # Define the dataVolumeTemplates content with parameters
            dvt_name = create_unique_resource_name("clone", "dvt")
            self.dv_cr_data_obj, self.dv_rb_data_obj = create_role(
                source_ns=source_vm_obj.namespace, dest_ns=self.namespace
            )
            vm_data["spec"]["dataVolumeTemplates"] = []
            metadata = {
                "name": dvt_name,
            }
            storage_spec = {
                "storage": {
                    "accessModes": [self.source_pvc_access_mode],
                    "resources": {"requests": {"storage": self.source_pvc_size}},
                },
                "source": {
                    "pvc": {
                        "namespace": source_vm_obj.namespace,
                        "name": source_vm_obj.source_pvc,
                    }
                },
            }

            vm_data["spec"]["dataVolumeTemplates"].append(
                {"metadata": metadata, "spec": storage_spec}
            )
            del vm_data["spec"]["template"]["spec"]["volumes"][0][
                "persistentVolumeClaim"
            ]
            vm_data["spec"]["template"]["spec"]["volumes"][0]["dataVolume"] = {
                "name": dvt_name
            }

        vm_ocs_obj = create_resource(**vm_data)
        logger.info(f"Successfully created VM: {vm_ocs_obj.name}")

        if verify:
            self.verify_vm(verify_ssh=ssh)

    def verify_vm(self, verify_ssh=False):
        """
        Verifies vm status and ssh connectivity if ssh is configured
        """
        self.wait_for_vm_status(status=constants.VM_RUNNING)
        if verify_ssh:
            self.wait_for_ssh_connectivity(timeout=1200)

    def get(self, out_yaml_format=True):
        """
        Get information about the VirtualMachine.

        Args:
            out_yaml_format (bool): True to get the output in YAML format.

        Returns:
            dict: Information about the VirtualMachine.

        """
        return self.vm_ocp_obj.get(
            resource_name=self._vm_name, out_yaml_format=out_yaml_format
        )

    def get_os_username(self):
        """
        Retrieve the operating system username from the cloud-init data associated with the virtual machine

        Returns:
            str: The operating system username

        Raises:
            UsernameNotFoundException: If the 'user' key is not present in the VM userData

        """
        vm_get_out = self.get()
        volumes = (
            vm_get_out.get("spec", {})
            .get("template", {})
            .get("spec", {})
            .get("volumes", [])
        )
        for volume in volumes:
            cloud_init_data = volume.get("cloudInitNoCloud") or volume.get(
                "cloudInitConfigDrive"
            )
            if cloud_init_data:
                user_data = cloud_init_data.get("userData", {})
                user_data_dict = yaml.safe_load(user_data)
                username = user_data_dict.get("user")
                if username is not None:
                    return username
                else:
                    raise UsernameNotFoundException(
                        f"Username not found in the {self.name} user data"
                    )

    def wait_for_vm_status(self, status=constants.VM_RUNNING, timeout=600):
        """
        Wait for the VirtualMachine to reach the specified status.

        Args:
            status (str): The desired status to wait for - Running/Stopped/Paused. default is "Running").
            timeout (int): Timeout value in seconds.

        Raises:
            TimeoutExpiredError: If the timeout is reached.

        """
        self.vm_ocp_obj.wait_for_resource(
            resource_name=self._vm_name, condition=status, timeout=timeout
        )

    def start(self, timeout=600, wait=True):
        """
        Start the VirtualMachine.

        Args:
            timeout (int): Timeout value in seconds.
            wait (bool): True to wait for the VirtualMachine to reach the "Running" status.

        """
        if (
            self.printableStatus() == constants.CNV_VM_STOPPED
            and self.check_if_vmi_does_not_exist()
        ):
            logger.info(
                f"{self._vm_name} is in stopped state and vmi does not exists, starting {self._vm_name}"
            )
        elif not self.check_if_vmi_does_not_exist():
            logger.info(
                f"VMI for this {self._vm_name} is still running, waiting for the vmi to "
                f"delete before starting the {self._vm_name}"
            )
            self.vmi_obj.wait_for_vmi_delete()
        self.start_vm(self._vm_name)
        logger.info(f"Successfully started VM: {self._vm_name}")

        if wait:
            self.wait_for_vm_status(status=constants.VM_RUNNING, timeout=timeout)
            logger.info(f"VM:{self._vm_name} reached Running state")

    def check_if_vmi_does_not_exist(self):
        """
        Check if the VirtualMachineInstance (VMI) does not exist.

        Returns:
            bool: True if the VMI does not exist.

        """
        status_conditions_out = self.get().get("status").get("conditions")[0]
        return status_conditions_out["reason"] == "VMINotExists"

    def wait_for_ssh_connectivity(self, username=None, timeout=600):
        """
        Wait for the SSH connectivity to establish to the virtual machine

        Args:
            username (str): The username to use for SSH. If None, it will use the OS username from vm_obj if exists
            timeout (int): The maximum time to wait for SSH connectivity in seconds

        """
        username = username if username else self.get_os_username()
        logger.info(f"Waiting for the SSH connectivity to establish to {self.name} ")
        for sample in TimeoutSampler(
            timeout=timeout,
            sleep=30,
            func=self.run_ssh_cmd,
            username=username,
            command="exit",
            use_sudo=False,
        ):
            if sample == "":
                logger.info(f"{self.name} is ready for SSH connection")
                return

    def stop(self, force=False, wait=True):
        """
        Stop the VirtualMachine.

        Args:
            force (bool): True to forcefully stop the VirtualMachine.
            wait (bool): True to wait for the VirtualMachine to reach the "Stopped" status.

        """
        self.stop_vm(self._vm_name, force=force)
        logger.info(f"Successfully stopped VM: {self._vm_name}")
        if wait:
            self.vmi_obj.wait_for_virt_launcher_pod_delete()
            self.vmi_obj.wait_for_vmi_delete()
            self.wait_for_vm_status(status=constants.CNV_VM_STOPPED)
            logger.info(f"VM: {self._vm_name} reached Stopped state")

    def restart(self, wait=True):
        """
        Restart the VirtualMachine.

        Args:
            wait (bool): True to wait for the VirtualMachine to reach the "Running" status.

        """
        self.restart_vm(self._vm_name)
        logger.info(f"Successfully restarted VM: {self._vm_name}")
        if wait:
            self.vmi_obj.wait_for_virt_launcher_pod_delete()
            self.vmi_obj.wait_for_vmi_to_be_running()
            logger.info(
                f"VM: {self._vm_name} reached Running state state after restart operation"
            )

    def addvolme(self, volume_name, persist=True, serial=None):
        """
        Add a volume to a VM

        Args:
            volume_name (str): Name of the volume/PVC to add.
            persist (bool): True to persist the volume.
            serial (str): Serial number for the volume.

        Returns:
             str: stdout of command

        """
        logger.info(f"Adding {volume_name} to {self._vm_name}")
        self.add_volume(
            vm_name=self._vm_name,
            volume_name=volume_name,
            persist=persist,
            serial=serial,
        )
        logger.info(f"Successfully HotPlugged disk {volume_name} to {self._vm_name}")

    def removevolume(self, volume_name):
        """
        Remove a volume from a VM

        Args:
            volume_name (str): Name of the volume to remove.

        Returns:
             str: stdout of command

        """
        logger.info(f"Removing {volume_name} from {self._vm_name}")
        self.remove_volume(vm_name=self._vm_name, volume_name=volume_name)
        logger.info(
            f"Successfully HotUnplugged disk {volume_name} from {self._vm_name}"
        )

    def scp_to_vm(
        self,
        local_path,
        vm_username=None,
        identity_file=None,
        vm_dest_path=None,
        recursive=False,
    ):
        """
        Copy files/directories from the local machine to the VirtualMachine using SCP.

        Args:
            local_path (str): Path to the local file/directory.
            vm_username (str): Username for SSH connection to the VirtualMachine.
            identity_file (str): Path to the SSH private key file.
            vm_dest_path (str): Destination path on the VirtualMachine.
            recursive (bool): True to copy directories recursively.

        Returns:
             str: stdout of command

        """
        vm_username = vm_username if vm_username else self.get_os_username()
        vm_dest_path = vm_dest_path if vm_dest_path else "."
        identity_file = (
            identity_file if identity_file else cnv_helpers.get_ssh_private_key_path()
        )
        logger.info(
            f"Starting scp from local machine path: {local_path} to VM path: {vm_dest_path}"
        )
        return self.scp(
            local_path,
            vm_username,
            self._vm_name,
            identity_file=identity_file,
            vm_dest_path=vm_dest_path,
            to_vm=True,
            recursive=recursive,
        )

    def scp_from_vm(
        self,
        local_path,
        vm_src_path,
        vm_username=None,
        identity_file=None,
        recursive=False,
    ):
        """
        Copy files/directories from the VirtualMachine to the local machine using SCP.

        Args:
            local_path (str): Path to the local destination.
            vm_username (str): Username for SSH connection to the VirtualMachine.
            identity_file (str): Path to the SSH private key file.
            vm_src_path (str): Source path on the VirtualMachine.
            recursive (bool): True to copy directories recursively.

        Returns:
             str: stdout of command

        """
        vm_username = vm_username if vm_username else self.get_os_username()
        identity_file = (
            identity_file if identity_file else cnv_helpers.get_ssh_private_key_path()
        )
        logger.info(
            f"Starting scp from VM path: {vm_src_path} to local machine path: {local_path}"
        )
        return self.scp(
            local_path,
            vm_username,
            self._vm_name,
            identity_file=identity_file,
            vm_dest_path=vm_src_path,
            to_vm=False,
            recursive=recursive,
        )

    def run_ssh_cmd(self, command, username=None, use_sudo=True, identity_file=None):
        """
        Connect to the VirtualMachine using SSH and execute a command.

        Args:
            username (str): SSH username for the VirtualMachine.
            command (str): Command to execute
            identity_file (str): Path to the SSH private key file.
            use_sudo (bool): True to run the command with sudo.

        Returns:
             str: stdout of command

        """
        logger.info(f"Executing {command} command on the {self._vm_name} VM using SSH")
        username = username if username else self.get_os_username()
        identity_file = (
            identity_file if identity_file else cnv_helpers.get_ssh_private_key_path()
        )
        return self.run_ssh_command(
            self._vm_name,
            username,
            command,
            use_sudo=use_sudo,
            identity_file=identity_file,
        )

    def pause(self, wait=True):
        """
        Pause the VirtualMachine.

        Args:
            wait (bool): True to wait for the VirtualMachine to reach the "Paused" status.

        """
        self._pause("vm", self._vm_name)
        logger.info(f"Successfully Paused VM: {self._vm_name}")
        if wait:
            self.wait_for_vm_status(status=constants.VM_PAUSED)
            logger.info(f"VM: {self._vm_name} reached Paused state")

    def unpause(self, wait=True):
        """
        Unpause the VirtualMachine.

        Args:
            wait (bool): True to wait for the VirtualMachine to reach the "Running" status.

        """
        self.unpause_vm(self._vm_name)
        logger.info(f"Successfully UnPaused VM: {self._vm_name}")
        if wait:
            self.wait_for_vm_status(status=constants.VM_RUNNING)
            logger.info(f"VM: {self._vm_name} reached Running state")

    def ready(self):
        """
        Get the readiness status of the VirtualMachine.

        Returns:
            bool: True if the VirtualMachine is ready.

        """
        return self.get().get("status", {}).get("ready")

    def printableStatus(self):
        """
        Get the printable status of the VirtualMachine.

        Returns:
            str: Printable status of the VirtualMachine.

        """
        return self.get().get("status").get("printableStatus")

    def delete(self):
        """
        Delete the VirtualMachine
        """
        if self.secret_obj:
            self.secret_obj.delete()
        self.vm_ocp_obj.delete(resource_name=self._vm_name)
        self.vm_ocp_obj.wait_for_delete(resource_name=self._vm_name, timeout=180)
        if self.volume_interface == constants.VM_VOLUME_PVC:
            self.pvc_obj.delete()
            self.volumeimportsource_obj.delete()
        if self.volume_interface == constants.VM_VOLUME_DV:
            self.dv_obj.delete()
        if self.ns_obj:
            self.ns_obj.delete_project(project_name=self.namespace)
