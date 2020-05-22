import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants, node, platform_nodes
from ocs_ci.framework.testlib import E2ETest, workloads, tier2
from ocs_ci.ocs.node import wait_for_nodes_status, get_node_objs, schedule_nodes
from ocs_ci.ocs.pgsql import Postgresql
from tests.conftest import mcg_obj, awscli_pod, bucket_factory, project_factory, multi_pvc_factory, pod_factory
from tests.e2e.flowtests import flow_test_helpers
from tests.e2e.flowtests.flow_test_helpers import test_create_delete_pvcs, test_create_pvc_delete, dc_pod_create_delete, \
    obc_put_obj_create_delete, pgsql_workload_start_verify
from tests.helpers import remove_label_from_worker_node
from tests.manage.mcg.helpers import obc_io_create_delete
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.resources import pod as pod_helpers


logger = logging.getLogger(__name__)

global OPERATION_COMPLETED


# setup function for workload (maybe)
# start workload fixture

def wrapper_obc_io_create_delete(
    mcg_obj,
    bucket_factory,
    iterations=1):
    """
    Args:
        mcg_obj(MCG): An MCG object containing the MCG S3 connection credentials
        awscli_pod:
        bucket_factory:
        iterations:
    Returns:
    """
    global OPERATION_COMPLETED
    for i in range(iterations):
        if OPERATION_COMPLETED:
            logging.info(f"wrapper_obc_io_create_delete: Done with execution. Stopping the thread. In iteration {i}")
            logger.info(f"####################################################################"
                        f"\n #######################       In iteration {i}       ####################"
                        f"\n ####################################################################")
            return True
        else:
            # there will be an assert if things go wrong in the functions
            # called by following function
            assert obc_put_obj_create_delete(mcg_obj, bucket_factory)
            logging.info(f"wrapper_obc_io_create_delete: iteration {i}")


def wrapper_pvc_create_delete(multi_pvc_factory, project, iterations=1):
    """
    Args:

        iterations:
    Returns:
    """
    global OPERATION_COMPLETED
    for i in range(iterations):
        if OPERATION_COMPLETED:
            logging.info(f"wrapper_pvc_create_delete: Done with execution. Stopping the thread. In iteration {i}")
            logger.info(f"####################################################################"
                        f"\n #######################       In iteration {i}       ####################"
                        f"\n ####################################################################")
            return True
        else:
            # there will be an assert if things go wrong in the functions
            # called by following function
            assert test_create_pvc_delete(multi_pvc_factory, project)
            logging.info(f"wrapper_pvc_create_delete: iteration {i}")

def wrapper_pgsql(pgsql, iterations=1):
    """
    Args:

        iterations:
    Returns:
    """
    global OPERATION_COMPLETED
    for i in range(iterations):
        if OPERATION_COMPLETED:
            logging.info(f"wrapper_pvc_create_delete: Done with execution. Stopping the thread. In iteration {i}")
            logger.info(f"####################################################################"
                        f"\n #######################       In iteration {i}       ####################"
                        f"\n ####################################################################")
            return True
        else:
            # there will be an assert if things go wrong in the functions
            # called by following function
            assert pgsql_workload_start_verify(pgsql)
            logging.info(f"wrapper_pvc_create_delete: iteration {i}")



@pytest.fixture(autouse=True)
def teardown(request):
    """
    Tear down function

    """

    def finalizer():
        """
        Make sure that all cluster's nodes are in 'Ready' state and if not,
        change them back to 'Ready' state by marking them as schedulable
        """
        scheduling_disabled_nodes = [
            n.name for n in get_node_objs() if n.ocp.get_resource_status(
                n.name
            ) == constants.NODE_READY_SCHEDULING_DISABLED
        ]
        if scheduling_disabled_nodes:
            schedule_nodes(scheduling_disabled_nodes)

        # Remove label created for DC app pods on all worker nodes
        node_objs = get_node_objs()
        for node_obj in node_objs:
            if 'dc' in node_obj.get().get('metadata').get('labels').keys():
                remove_label_from_worker_node(
                    [node_obj.name], label_key="dc"
                )

    request.addfinalizer(finalizer)


@pytest.fixture(scope='function')
def pgsql(request):
    pgsql = Postgresql()
    pgsql.setup_postgresql(replicas=3)

    def teardown():
        pgsql.cleanup()

    request.addfinalizer(teardown)
    return pgsql


@tier2
@pytest.mark.polarion_id("OCS-xsadsa")
class TestStoryBasedTests(E2ETest):
    """
    Test
    """
    
    def test_base_operation_add_node(self, project_factory, mcg_obj, bucket_factory, multi_pvc_factory):
    # def test_base_operation_add_node(self, project_factory, multi_pvc_factory, pod_factory):
        """
        Test ......
        """
        global OPERATION_COMPLETED

        # add node

        # Add disk (capacity)

        project = project_factory()

        OPERATION_COMPLETED = False

        # pgsql.create_pgbench_benchmark(replicas=3, clients=3, transactions=100)

        executor_run_bg_ios_ops = ThreadPoolExecutor(max_workers=2)

        # executor_run_bg_ios_ops.submit(dc_pod_create_delete, multi_dc_pod, project)

        logging.info("Started obc_io_create_delete...")
        obc_ios = executor_run_bg_ios_ops.submit(
            wrapper_obc_io_create_delete, mcg_obj, bucket_factory, 30
        )

        logging.info("Started pvc create delete...")
        pvc_create_delete = executor_run_bg_ios_ops.submit(wrapper_pvc_create_delete, multi_pvc_factory, project, 20)

        logging.info("Started PGSQL IOS...")
        pgsql_create_delete = executor_run_bg_ios_ops.submit(pgsql, 10)

        logger.info(f"####################################################################"
                    f"\n #######################       NODE DRAIN        ####################"
                    f"\n ####################################################################")

        # node_name, restart_count_before = flow_test_helpers.node_drain_setup(node_type='worker', number_of_nodes=1)

        node_name = flow_test_helpers.node_drain_setup(node_type='worker', number_of_nodes=1)

        # Node maintenance - to gracefully terminate all pods on the node
        node.drain_nodes([node_name])

        # Make the node schedulable again
        node.schedule_nodes([node_name])

        # Perform cluster and Ceph health checks
        # flow_test_helpers.node_drain_exit(restart_count_before)

        flow_test_helpers.node_drain_exit()

    # # for node in worker_nodes:
        # logger.info(f"####################################################################"
        #         f"\n #######################       NODE RESTART        ####################"
        #         f"\n ####################################################################")

        # Node failure (reboot)
        # Rolling reboot on worker nodes

        # worker_nodes = node.get_typed_nodes(node_type='worker', num_of_nodes=1)
        #
        # factory = platform_nodes.PlatformNodesFactory()
        # nodes = factory.get_nodes_platform()
        #
        #
        #
        # nodes.restart_nodes(nodes=worker_nodes)
        # wait_for_nodes_status()

        # pgbench_pods = pgsql.get_pgbench_pods()
        # pgsql.wait_for_pgbench_status(status=constants.STATUS_COMPLETED)
        # pgsql.validate_pgbench_run(pgbench_pods)

        OPERATION_COMPLETED = True

        from time import sleep
        while not (obc_ios.done() and pvc_create_delete.done() and pgsql_create_delete.done()):
            sleep(10)
            logger.info(
                "#### WAITING FOR OBC_IOS, PVC CREATE DELETE FINAL ITERATION TO COMPLETE")

        logger.info("passed")

# teardown fixture verifying before deleting the workload
