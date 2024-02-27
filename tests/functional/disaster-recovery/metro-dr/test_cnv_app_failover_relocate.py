import logging
import pytest
import time


from ocs_ci.framework.pytest_customization.marks import tier2
from ocs_ci.framework import config
from ocs_ci.helpers.cnv_helpers import create_vm_secret
from ocs_ci.ocs import constants
from ocs_ci.ocs.cnv.virtual_machine import VirtualMachine
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs
from ocs_ci.helpers.dr_helpers import (
    enable_fence,
    enable_unfence,
    get_fence_state,
    failover,
    relocate,
    set_current_primary_cluster_context,
    set_current_secondary_cluster_context,
    get_current_primary_cluster_name,
    get_current_secondary_cluster_name,
    wait_for_all_resources_creation,
    wait_for_all_resources_deletion,
    gracefully_reboot_ocp_nodes,
    wait_for_cnv_workload,
)

from ocs_ci.framework.pytest_customization.marks import turquoise_squad

logger = logging.getLogger(__name__)

polarion_id_cnv_primary_up = "OCS-5413"
polarion_id_cnv_primary_down = "OCS-5414"


@tier2
@turquoise_squad
class TestCnvApplicationMDR:
    """
    Includes tests related to CNV workloads on MDR environment.
    """

    # @pytest.fixture(autouse=True)
    # def teardown(self, request, cnv_dr_workload):
    #     """
    #     Teardown function: If fenced, un-fence the cluster and reboot nodes
    #     """
    #
    #     def finalizer():
    #         if (
    #             self.primary_cluster_name
    #             and get_fence_state(self.primary_cluster_name) == "Fenced"
    #         ):
    #             enable_unfence(self.primary_cluster_name)
    #             gracefully_reboot_ocp_nodes(
    #                 self.wl_namespace, self.primary_cluster_name
    #             )
    #
    #     request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=["primary_cluster_down"],
        argvalues=[
            # pytest.param(
            #     False,
            #     marks=pytest.mark.polarion_id(polarion_id_cnv_primary_up),
            #     id="primary_up",
            # ),
            pytest.param(
                True,
                marks=pytest.mark.polarion_id(polarion_id_cnv_primary_down),
                id="primary_down",
            ),
        ],
    )
    def test_cnv_app_failover_relocate(
        self,
        primary_cluster_down,
        nodes_multicluster,
        cnv_dr_workload,
        # node_restart_teardown,
    ):
        """
        Tests to verify CNV based subscription and appset application deployment and
        fail-over/relocate between managed clusters.

        """
        # Create CNV applications(appset+sub)
        cnv_workloads = cnv_dr_workload(num_of_vm_subscription=1, num_of_vm_appset=1)
        self.wl_namespace = cnv_workloads[0].workload_namespace

        set_current_primary_cluster_context(
            self.wl_namespace, cnv_workloads[0].workload_type
        )
        primary_cluster_index = config.cur_index

        self.primary_cluster_name = get_current_primary_cluster_name(
            namespace=self.wl_namespace, workload_type=cnv_workloads[0].workload_type
        )
        # vm_name = "vm-workload-1"
        # self.wl_namespace = "vm-sub-1"

        md5sum_original = []
        vm_obj = []
        for cnv_wl in cnv_workloads:
            vm_obj.append(VirtualMachine(vm_name=cnv_wl.vm_name, namespace=cnv_wl.workload_namespace))
            # config.switch_to_cluster_by_name("pbyregow-x1")
            # vm_obj = VirtualMachine(vm_name=cnv_wl.vm_name, namespace=cnv_wl.workload_namespace)
            # vm_obj = VirtualMachine(vm_name=vm_name, namespace=self.wl_namespace)

            # create_vm_secret(name="vm-secret-1", namespace=self.wl_namespace)
        for obj in vm_obj:
            obj.run_ssh_cmd(
                command="dd if=/dev/zero of=/dd_file.txt bs=1024 count=102400"
            )
            md5sum_cmd_out = obj.run_ssh_cmd(
                command="md5sum /dd_file.txt"
            )
            md5sum_original.append(md5sum_cmd_out.split()[0])
            logger.info(md5sum_original)

        # Shutting down primary cluster nodes
        node_objs = get_node_objs()
        if primary_cluster_down:
            logger.info("Stopping primary cluster nodes")
            nodes_multicluster[primary_cluster_index].stop_nodes(node_objs)

        # Fence the primary managed cluster
        enable_fence(drcluster_name=self.primary_cluster_name)

        secondary_cluster_name = get_current_secondary_cluster_name(
            self.wl_namespace, cnv_workloads[0].workload_type
        )

        # TODO: Write a file or any IO inside VM

        # Fail-over the apps to secondary managed cluster
        for cnv_wl in cnv_workloads:
            failover(
                failover_cluster=secondary_cluster_name,
                namespace=cnv_wl.workload_namespace,
                workload_type=cnv_wl.workload_type,
                workload_placement_name=cnv_wl.cnv_workload_placement_name
                if cnv_wl.workload_type != constants.SUBSCRIPTION
                else None,
            )

        # Verify VM and its resources in secondary managed cluster
        set_current_primary_cluster_context(
            self.wl_namespace, cnv_workloads[0].workload_type
        )
        for cnv_wl in cnv_workloads:
            wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
            )
            wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        set_current_primary_cluster_context(
            self.wl_namespace, cnv_workloads[0].workload_type
        )
        for count, vm in enumerate(vm_obj):
            # ctx = config.cur_index
            md5sum_fail_out = vm.run_ssh_cmd(command="md5sum /dd_file.txt", username="cirros",)
            md5sum_fail = md5sum_fail_out.split()[0]
            if md5sum_original[count] == md5sum_fail:
                 logger.info(
                    f"Passed: MD5 comparison "
                )

        # Start nodes if cluster is down
        wait_time = 120
        if primary_cluster_down:
            logger.info(
                f"Waiting for {wait_time} seconds before starting nodes of previous primary cluster"
            )
            time.sleep(wait_time)
            nodes_multicluster[primary_cluster_index].start_nodes(node_objs)
            logger.info(
                f"Waiting for {wait_time} seconds after starting nodes of previous primary cluster"
            )
            time.sleep(wait_time)
            wait_for_nodes_status([node.name for node in node_objs])

        # Verify application are deleted from old managed cluster
        set_current_secondary_cluster_context(
            cnv_workloads[0].workload_namespace, cnv_workloads[0].workload_type
        )
        for cnv_wl in cnv_workloads:
            wait_for_all_resources_deletion(cnv_wl.workload_namespace)

        # TODO: Validate Data integrity

        # Un-fence the managed cluster
        enable_unfence(drcluster_name=self.primary_cluster_name)

        # Reboot the nodes after unfenced
        gracefully_reboot_ocp_nodes(
            self.wl_namespace, self.primary_cluster_name, cnv_workloads[0].workload_type
        )

        secondary_cluster_name = get_current_secondary_cluster_name(
            self.wl_namespace, cnv_workloads[0].workload_type
        )

        # Relocate cnv apps back to primary managed cluster
        for cnv_wl in cnv_workloads:
            relocate(
                preferred_cluster=secondary_cluster_name,
                namespace=cnv_wl.workload_namespace,
                workload_type=cnv_wl.workload_type,
                workload_placement_name=cnv_wl.cnv_workload_placement_name
                if cnv_wl.workload_type != constants.SUBSCRIPTION
                else None,
            )

        set_current_secondary_cluster_context(
            self.wl_namespace, cnv_workloads[0].workload_type
        )
        # Verify resources deletion from previous primary or current secondary cluster
        for cnv_wl in cnv_workloads:
            wait_for_all_resources_deletion(cnv_wl.workload_namespace)

        # Verify resource creation and VM status on relocated cluster
        set_current_primary_cluster_context(
            self.wl_namespace, cnv_workloads[0].workload_type
        )
        for cnv_wl in cnv_workloads:
            wait_for_all_resources_creation(
                cnv_wl.workload_pvc_count,
                cnv_wl.workload_pod_count,
                cnv_wl.workload_namespace,
            )
            wait_for_cnv_workload(
                vm_name=cnv_wl.vm_name,
                namespace=cnv_wl.workload_namespace,
                phase=constants.STATUS_RUNNING,
            )

        set_current_primary_cluster_context(
            self.wl_namespace, cnv_workloads[0].workload_type
        )
        for count, vm in enumerate(vm_obj):
            set_current_primary_cluster_context(
                self.wl_namespace, cnv_workloads[0].workload_type
            )
            md5sum_relo_out = vm.run_ssh_cmd(username="cirros",
                command="md5sum /dd_file.txt"
            )
            md5sum_relo = md5sum_relo_out.split()[0]
            if md5sum_original[count] == md5sum_relo:
                 logger.info(
                    f"Passed: MD5 comparison "
                )
        # TODO: Validate Data integrity
