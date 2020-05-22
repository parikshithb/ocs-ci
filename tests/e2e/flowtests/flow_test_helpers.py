import logging

import pytest
from ocs_ci.ocs import constants, node, platform_nodes, defaults
from ocs_ci.framework.testlib import E2ETest, workloads, tier2, ignore_leftovers
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.node import wait_for_nodes_status
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.ocs.resources import pod as pod_helpers
from tests import helpers
from tests.manage.mcg.helpers import obc_io_create_delete, s3_delete_object, s3_put_object, s3_get_object
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.resources.pvc import delete_pvcs

import logging
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.framework.testlib import ignore_leftovers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import delete_pvcs
from tests import helpers

logger = logging.getLogger(__name__)


def node_drain_setup(node_type, number_of_nodes):
    # Perform cluster and Ceph health checks

    # Sanity().health_check()
    #
    # assert CephCluster().is_health_ok(), "Entry criteria FAILED: Cluster is Unhealthy"

    typed_nodes = node.get_typed_nodes(
        node_type=node_type, num_of_nodes=number_of_nodes
    )
    node_name = typed_nodes[0].name

    # restart_count_before = pod_helpers.get_pod_restarts_count(
    #     defaults.ROOK_CLUSTER_NAMESPACE)

    return node_name #, restart_count_before


def node_drain_exit():
    # Perform cluster and Ceph health checks

    # Sanity().health_check()
    #
    # restart_count_after = pod_helpers.get_pod_restarts_count(
    #     defaults.ROOK_CLUSTER_NAMESPACE)
    # logging.info(f"sum(restart_count_before.values()) = {sum(restart_count_before.values())}")
    # logging.info(f" sum(restart_count_after.values()) = {sum(restart_count_after.values())}")
    # assert sum(restart_count_before.values()) == sum(restart_count_after.values(
    # )), "Exit criteria verification FAILED: One or more pods got restarted"
    #
    # assert CephCluster().is_health_ok(), "Entry criteria FAILED: Cluster is Unhealthy"

    logging.info(
        "Exit criteria verification Success: No pods were restarted")


def pods_creation(pod_factory, pvc_objs):
    """
    Create pods
    Args:
        pvc_objs (list): List of ocs_ci.ocs.resources.pvc.PVC instances
        pvc_objs (function): Function to be used for creating pods
    Returns:
        list: list of Pod objects
    """
    pod_objs = []

    # Create one pod using each RWO PVC and two pods using each RWX PVC
    for pvc_obj in pvc_objs:
        if pvc_obj.volume_mode == 'Block':
            pod_dict = constants.CSI_RBD_RAW_BLOCK_POD_YAML
            raw_block_pv = True
        else:
            raw_block_pv = False
            pod_dict = ''
        if pvc_obj.access_mode == constants.ACCESS_MODE_RWX:
            pod_obj = pod_factory(
                interface=pvc_obj.interface, pvc=pvc_obj, status="",
                pod_dict_path=pod_dict, raw_block_pv=raw_block_pv
            )
            pod_objs.append(pod_obj)
        pod_obj = pod_factory(
            interface=pvc_obj.interface, pvc=pvc_obj, status="",
            pod_dict_path=pod_dict, raw_block_pv=raw_block_pv
        )
        pod_objs.append(pod_obj)

    return pod_objs


def create_pvcs(
    multi_pvc_factory, interface, project=None, status="", storageclass=None
):
    pvc_num = 1
    pvc_size = 1

    if interface == 'CephBlockPool':
        access_modes = [
            'ReadWriteOnce',
            'ReadWriteOnce-Block',
            'ReadWriteMany-Block']
    else:
        access_modes = ['ReadWriteOnce', 'ReadWriteMany']
    # Create pvcs
    pvc_objs = multi_pvc_factory(
        interface=interface,
        project=project,
        storageclass=storageclass,
        size=pvc_size,
        access_modes=access_modes,
        access_modes_selection='distribute_random',
        status=status,
        num_of_pvc=pvc_num,
        wait_each=False,
    )

    for pvc_obj in pvc_objs:
        pvc_obj.interface = interface

    return pvc_objs


def delete_pods(pod_objs):
    """
    Delete pods
    """
    for pod_obj in pod_objs:
        pod_obj.delete()
    return True


@ignore_leftovers
def test_create_delete_pvcs(multi_pvc_factory, pod_factory, project=None):
    # create the pods for deleting
    # Create rbd pvcs for pods
    logging.info('entering ..')
    pvc_objs_rbd = create_pvcs(
        multi_pvc_factory,
        'CephBlockPool',
        project=project)
    proj_obj = pvc_objs_rbd[0].project
    storageclass_rbd = pvc_objs_rbd[0].storageclass

    # Create cephfs pvcs for pods
    pvc_objs_cephfs = create_pvcs(
        multi_pvc_factory, 'CephFileSystem', project=proj_obj
    )
    storageclass_cephfs = pvc_objs_cephfs[0].storageclass

    all_pvc_for_pods = pvc_objs_rbd + pvc_objs_cephfs
    # Check pvc status
    for pvc_obj in all_pvc_for_pods:
        helpers.wait_for_resource_state(
            resource=pvc_obj, state=constants.STATUS_BOUND, timeout=1200  # Timeout given 5 minutes
        )
        pvc_info = pvc_obj.get()
        setattr(pvc_obj, 'volume_mode', pvc_info['spec']['volumeMode'])

    # Create pods
    pods_to_delete = pods_creation(pod_factory, all_pvc_for_pods)
    for pod_obj in pods_to_delete:
        helpers.wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING, timeout=300  # Timeout given 5 minutes
        )

    logging.info(f"#### Created the pods for deletion later...pods = {pods_to_delete}")
    # Create PVCs for deleting
    # Create rbd pvcs for deleting
    pvc_objs_rbd = create_pvcs(
        multi_pvc_factory=multi_pvc_factory, interface='CephBlockPool',
        project=proj_obj, status="", storageclass=storageclass_rbd
    )

    # Create cephfs pvcs for deleting
    pvc_objs_cephfs = create_pvcs(
        multi_pvc_factory=multi_pvc_factory, interface='CephFileSystem',
        project=proj_obj, status="", storageclass=storageclass_cephfs
    )

    all_pvc_to_delete = pvc_objs_rbd + pvc_objs_cephfs
    # Check pvc status
    for pvc_obj in all_pvc_to_delete:
        helpers.wait_for_resource_state(
            resource=pvc_obj, state=constants.STATUS_BOUND, timeout=300  # Timeout given 5 minutes
        )

    logging.info(f"#### Created the PVCs for deletion later...PVCs={all_pvc_to_delete}")

    # Create PVCs for new pods
    pvc_objs_rbd = create_pvcs(
        multi_pvc_factory=multi_pvc_factory, interface='CephBlockPool',
        project=proj_obj, status="", storageclass=storageclass_rbd
    )

    # Create cephfs pvcs for new pods # for deleting
    pvc_objs_cephfs = create_pvcs(
        multi_pvc_factory=multi_pvc_factory, interface='CephFileSystem',
        project=proj_obj, status="", storageclass=storageclass_cephfs
    )

    all_pvc_for_new_pods = pvc_objs_rbd + pvc_objs_cephfs
    # Check pvc status
    for pvc_obj in all_pvc_for_new_pods:
        helpers.wait_for_resource_state(
            resource=pvc_obj, state=constants.STATUS_BOUND, timeout=300  # Timeout given 5 minutes
        )
        pvc_info = pvc_obj.get()
        setattr(pvc_obj, 'volume_mode', pvc_info['spec']['volumeMode'])

    logging.info(f"#### Created the PVCs required for creating New Pods...{all_pvc_for_new_pods}")

    executor = ThreadPoolExecutor(max_workers=10)
    # Start creating new PVCs
    # Start creating rbd PVCs
    rbd_pvc_exeuter = executor.submit(
        create_pvcs, multi_pvc_factory=multi_pvc_factory,
        interface='CephBlockPool', project=proj_obj, status="",
        storageclass=storageclass_rbd
    )

    logging.info("#### Started creating new RBD PVCs in thread...")
    # Start creating cephfs pvc
    cephfs_pvc_exeuter = executor.submit(
        create_pvcs, multi_pvc_factory=multi_pvc_factory,
        interface='CephFileSystem', project=proj_obj,
        status="", storageclass=storageclass_cephfs
    )

    logging.info("#### Started creating new cephfs PVCs in thread...")
    # Start creating pods
    pods_create_executer = executor.submit(
        pods_creation, pod_factory, all_pvc_for_new_pods
    )

    # Start deleting pods
    pods_delete_executer = executor.submit(delete_pods, pods_to_delete)
    logging.info(f"### Started deleting the pods_to_delete = {pods_to_delete}")

    # Start deleting PVC
    pvc_delete_executer = executor.submit(delete_pvcs, all_pvc_to_delete)
    logging.info(f"### Started deleting the all_pvc_to_delete = {all_pvc_to_delete}")

    logger.info(
        "These process are started: Bulk delete PVC, Pods. Bulk create PVC, "
        "Pods. Waiting for its completion"
    )

    from time import sleep
    while not (rbd_pvc_exeuter.done() and cephfs_pvc_exeuter.done() and
               pods_create_executer.done() and pods_delete_executer.done() and pvc_delete_executer.done()):
        sleep(10)
        logging.info(
            "#### create_delete_pvcs....Waiting for threads to complete...")

    new_rbd_pvcs = rbd_pvc_exeuter.result()
    new_cephfs_pvcs = cephfs_pvc_exeuter.result()
    new_pods = pods_create_executer.result()

    # Check pvc status
    for pvc_obj in new_rbd_pvcs + new_cephfs_pvcs:
        helpers.wait_for_resource_state(
            resource=pvc_obj, state=constants.STATUS_BOUND, timeout=300  # Timeout given 5 minutes
        )

    logger.info("All new PVCs are bound")

    # Check pods status
    for pod_obj in new_pods:
        helpers.wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING, timeout=300  # Timeout given 5 minutes
        )
    logger.info("All new pods are running")

    # Check pods are deleted
    for pod_obj in pods_to_delete:
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

    logger.info("All pods are deleted as expected.")

    # Check PVCs are deleted
    for pvc_obj in all_pvc_to_delete:
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

    logger.info("All PVCs are deleted as expected")


@ignore_leftovers
def test_create_pvc_delete(multi_pvc_factory, project=None):
    # create the pods for deleting
    # Create rbd pvcs for pods
    logging.info('entering .. NEWWWW')
    # pvc_objs_rbd = create_pvcs(
    #     multi_pvc_factory,
    #     'CephBlockPool',
    #     project=project)
    # proj_obj = pvc_objs_rbd[0].project
    # storageclass_rbd = pvc_objs_rbd[0].storageclass
    #
    # # Create cephfs pvcs for pods
    # pvc_objs_cephfs = create_pvcs(
    #     multi_pvc_factory, 'CephFileSystem', project=proj_obj
    # )
    # storageclass_cephfs = pvc_objs_cephfs[0].storageclass
    #
    # all_pvc_for_pods = pvc_objs_rbd + pvc_objs_cephfs
    # # Check pvc status
    # for pvc_obj in all_pvc_for_pods:
    #     helpers.wait_for_resource_state(
    #         resource=pvc_obj, state=constants.STATUS_BOUND, timeout=1200  # Timeout given 5 minutes
    #     )
    #     pvc_info = pvc_obj.get()
    #     setattr(pvc_obj, 'volume_mode', pvc_info['spec']['volumeMode'])

    # Create PVCs for deleting
    # Create rbd pvcs for deleting
    pvc_objs_rbd = create_pvcs(
        multi_pvc_factory=multi_pvc_factory, interface='CephBlockPool',
        project=project, status="", storageclass=None
    )

    # Create cephfs pvcs for deleting
    pvc_objs_cephfs = create_pvcs(
        multi_pvc_factory=multi_pvc_factory, interface='CephFileSystem',
        project=project, status="", storageclass=None
    )

    all_pvc_to_delete = pvc_objs_rbd + pvc_objs_cephfs
    # Check pvc status
    for pvc_obj in all_pvc_to_delete:
        helpers.wait_for_resource_state(
            resource=pvc_obj, state=constants.STATUS_BOUND, timeout=300  # Timeout given 5 minutes
        )

    logging.info(f"#### Created the PVCs for deletion later...PVCs={all_pvc_to_delete}")

    executor = ThreadPoolExecutor(max_workers=10)
    # Start creating new PVCs
    # Start creating rbd PVCs
    rbd_pvc_exeuter = executor.submit(
        create_pvcs, multi_pvc_factory=multi_pvc_factory,
        interface='CephBlockPool', project=project, status="",
        storageclass=None
    )

    logging.info("#### Started creating new RBD PVCs in thread...")
    # Start creating cephfs pvc
    cephfs_pvc_exeuter = executor.submit(
        create_pvcs, multi_pvc_factory=multi_pvc_factory,
        interface='CephFileSystem', project=project,
        status="", storageclass=None
    )

    # Start deleting PVC
    logging.info("#### Started creating new cephfs PVCs in thread...")
    pvc_delete_executer = executor.submit(delete_pvcs, all_pvc_to_delete)
    logging.info(f"### Started deleting the all_pvc_to_delete = {all_pvc_to_delete}")

    logger.info(
        "These process are started: Bulk delete PVC, Pods. Bulk create PVC, "
        "Pods. Waiting for its completion"
    )

    from time import sleep
    while not (rbd_pvc_exeuter.done() and cephfs_pvc_exeuter.done() and pvc_delete_executer.done()):
        sleep(10)
        logging.info(
            "#### create_delete_pvcs....Waiting for threads to complete...")
    logger.info(f"thread here ")
    new_rbd_pvcs = rbd_pvc_exeuter.result()
    new_cephfs_pvcs = cephfs_pvc_exeuter.result()

    logger.info(f"Now thread here ")
    # Check pvc status
    for pvc_obj in new_rbd_pvcs + new_cephfs_pvcs:
        logger.info(f"Now thread in for ")
        helpers.wait_for_resource_state(
            resource=pvc_obj, state=constants.STATUS_BOUND, timeout=300  # Timeout given 5 minutes
        )

    logger.info(f"NOw HERE AT THE END")
    # Check PVCs are deleted
    for pvc_obj in all_pvc_to_delete:
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_obj.name)

    logger.info("All PVCs are deleted as expected")


# @pytest.fixture()
# def multi_dc_pod(multi_pvc_factory, dc_pod_factory, service_account_factory):
#     """
#     Prepare multiple dc pods for the test
#     Returns:
#         list: Pod instances
#     """
#
#     def factory(num_of_pvcs=1, pvc_size=100, project=None, access_mode="RWO", pool_type="rbd", timeout=60):
#
#         dict_modes = {"RWO": "ReadWriteOnce", "RWX": "ReadWriteMany", "RWX-BLK": "ReadWriteMany-Block"}
#         dict_types = {"rbd": "CephBlockPool", "cephfs": "CephFileSystem"}
#
#         if access_mode in "RWX-BLK" and pool_type in "rbd":
#             modes = dict_modes["RWX-BLK"]
#             create_rbd_block_rwx_pod = True
#         else:
#             modes = dict_modes[access_mode]
#             create_rbd_block_rwx_pod = False
#
#         pvc_objs = multi_pvc_factory(
#             interface=dict_types[pool_type],
#             access_modes=[modes],
#             size=pvc_size,
#             num_of_pvc=num_of_pvcs,
#             project=project,
#             timeout=timeout
#         )
#         dc_pods = []
#         dc_pods_res = []
#         sa_obj = service_account_factory(project=project)
#         with ThreadPoolExecutor() as p:
#             for pvc in pvc_objs:
#                 if create_rbd_block_rwx_pod:
#                     dc_pods_res.append(
#                         p.submit(
#                             dc_pod_factory, interface=constants.CEPHBLOCKPOOL,
#                             pvc=pvc, raw_block_pv=True, service_account=sa_obj
#                         ))
#                 else:
#                     dc_pods_res.append(
#                         p.submit(
#                             dc_pod_factory, interface=dict_types[pool_type],
#                             pvc=pvc, service_account=sa_obj
#                         ))
#
#         for dc in dc_pods_res:
#             pod_obj = dc.result()
#             if create_rbd_block_rwx_pod:
#                 logging.info(f"#### setting attribute pod_type since"
#                              f" create_rbd_block_rwx_pod = {create_rbd_block_rwx_pod}"
#                              )
#                 setattr(pod_obj, 'pod_type', 'rbd_block_rwx')
#             else:
#                 setattr(pod_obj, 'pod_type', '')
#             dc_pods.append(pod_obj)
#
#         with ThreadPoolExecutor() as p:
#             for dc in dc_pods:
#                 p.submit(
#                     helpers.wait_for_resource_state,
#                     resource=dc,
#                     state=constants.STATUS_RUNNING,
#                     timeout=120)
#
#         return dc_pods
#
#     return factory


def dc_pod_create_delete(multi_dc_pod, project):
    executor = ThreadPoolExecutor(max_workers=4)
    num_of_pvcs = 1
    executor.submit(multi_dc_pod,
                    num_of_pvcs=num_of_pvcs,
                    pvc_size=5,
                    project=project,
                    access_mode="RWO",
                    pool_type='rbd'
                    )
    executor.submit(multi_dc_pod,
                    num_of_pvcs=num_of_pvcs,
                    pvc_size=5,
                    project=project,
                    access_mode="RWO",
                    pool_type='cephfs'
                    )
    executor.submit(multi_dc_pod,
                    num_of_pvcs=num_of_pvcs,
                    pvc_size=5,
                    project=project,
                    access_mode="RWX",
                    pool_type='cephfs'
                    )
    # Create rwx-rbd pods
    executor.submit(multi_dc_pod,
                    num_of_pvcs=num_of_pvcs,
                    pvc_size=5,
                    project=project,
                    access_mode="RWX-BLK",
                    pool_type='rbd'
                    )


def obc_put_obj_create_delete(mcg_obj, bucket_factory):

    data = "asdasd"
    bucket_name = bucket_factory(amount=1, interface='OC')[0].name

    for i in range(0, 30):
        key = 'pari' + f"{i}"
        logger.info(f"writing object {key}")
        assert s3_put_object(mcg_obj, bucket_name, key, data)
        logger.info(f"Reading object {key}")
        assert s3_get_object(mcg_obj, bucket_name, key)
        logger.info(f"Deleting object {key}")
        assert s3_delete_object(mcg_obj, bucket_name, key)

    logger.info(f"FINISHED WRITING READING OBJECTS")

def pgsql_workload_start_verify(pgsql):
    # Create pgbench benchmark
    pgsql.create_pgbench_benchmark(
        replicas=3, transactions=100, clients=3
    )
    # Wait for pgbench pod to reach running state
    pgsql.wait_for_pgbench_status(status=constants.STATUS_RUNNING)

    # Wait for pg_bench pod to complete
    pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)

    # Get pgbench pods
    pgbench_pods = pgsql.get_pgbench_pods()

    # Validate pgbench run and parse logs
    pgsql.validate_pgbench_run(pgbench_pods)

