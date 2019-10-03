
import mock
import numbers
import unittest
import copy
import json


import builder.build
import builder.execution
from builder.tests.tests_jobs import *
from builder.build import BuildManager
from builder.execution import Executor, ExecutionManager, ExecutionResult, _submit_from_json
from builder.expanders import TimestampExpander

import builder.util as util
arrow = util.arrow_factory


class ExtendedMockExecutor(Executor):
    """"Executes" by running the jobs effect. An effect is a dictionary of
    things to do. Here is an example effect
    { }
    This effect updates non of the targets

    Here is another effect
    {
        "A-target": 500
    }
    This effect set's A's target's do_get_mtime to a mock thar returns 500
    """
    should_update_build_graph = True

    def do_execute(self, job):
        build_graph = self.get_build_graph()
        command = job.get_command()
        effect = job.get_effect()
        if isinstance(effect, numbers.Number):
            success = True
        else:
            success = effect.get('success') or True

        target_ids = build_graph.get_target_ids(job.get_id())
        for target_id in target_ids:
            target = build_graph.get_target(target_id)
            if isinstance(effect, numbers.Number):
                target.do_get_mtime = mock.Mock(return_value=effect)
            elif target_id not in effect:
                continue
            else:
                target.do_get_mtime = mock.Mock(return_value=effect[target_id])
            print target_id, target.do_get_mtime()

        result = ExecutionResult(False, success, command, command)
        return result


class ExecutionManagerTests1(unittest.TestCase):

    def _get_execution_manager(self, jobs, executor=None):
        build_manager = builder.build.BuildManager(jobs=jobs, metas=[])

        if executor is None:
            executor = mock.Mock(return_value=mock.Mock(status=True, stdout='', stderr=''))
        execution_manager = builder.execution.ExecutionManager(build_manager, executor)

        return execution_manager


    def _get_buildable_job(self):
        return EffectJobDefinition("buildable_job", expander_type=TimestampExpander,
                depends=[{"unexpanded_id": "buildable_15_minute_target_01-%Y-%m-%d-%H-%M", "file_step": "15min"},
                    {"unexpanded_id": "buildable_5_minute_target_01-%Y-%m-%d-%H-%M", "file_step": "5min"},
                    {"unexpanded_id": "buildable_15_minute_target_02-%Y-%m-%d-%H-%M", "file_step": "15min",
                        "type": "depends_one_or_more"},
                    {"unexpanded_id": "buildable_5_minute_target_02-%Y-%m-%d-%H-%M", "file_step": "5min",
                        "type": "depends_one_or_more"}])

    def test_submit(self):
        # Given
        execution_manager = self._get_execution_manager([self._get_buildable_job()])
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.running = True
        execution_manager.submit('buildable_job', build_context)

        # Then
        self.assertIn('buildable_job', execution_manager.get_build())


    def test_start_excution_run_to_completion(self):
        # Given
        execution_manager = self._get_execution_manager([self._get_buildable_job()], executor=ExtendedMockExecutor)
        execution_manager.executor.execute = mock.Mock(wraps=execution_manager.executor.execute)
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.build.add_job('buildable_job', build_context, force=True)
        execution_manager.start_execution(inline=True)

        # Then
        self.assertTrue(execution_manager.executor.execute.called)

    def test_inline_execution_simple_plan(self):
        # Given
        jobs = [
            EffectJobDefinition('A', targets=['target-A']),
            EffectJobDefinition('B', depends=['target-A'], targets=['target-B1', 'target-B2'])
        ]
        execution_manager = self._get_execution_manager(jobs, executor=ExtendedMockExecutor)
        execution_manager.executor.execute = mock.Mock(wraps=execution_manager.executor.execute)
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.build.add_job('B', build_context)
        execution_manager.start_execution(inline=True)

        # Then
        self.assertEquals(execution_manager.executor.execute.call_count, 2)

    def test_inline_execution_retries(self):
        # Given
        jobs = [
            EffectJobDefinition('A', targets=['target-A'], effect={"target-A": None}),
        ]
        execution_manager = self._get_execution_manager(jobs, executor=ExtendedMockExecutor)
        execution_manager.executor.execute = mock.Mock(wraps=execution_manager.executor.execute)
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.build.add_job('A', build_context)

        execution_manager.start_execution(inline=True)

        # Then
        self.assertEquals(execution_manager.executor.execute.call_count, 5)

class ExecutionManagerTests2(unittest.TestCase):

    def _get_execution_manager(self, jobs):
        build_manager = BuildManager(jobs, metas=[])
        execution_manager = ExecutionManager(build_manager, lambda execution_manager, config=None: ExtendedMockExecutor(execution_manager, config=config))
        return execution_manager

    def _get_execution_manager_with_effects(self, jobs):
        build_manager = BuildManager(jobs, metas=[])
        execution_manager = ExecutionManager(build_manager, ExtendedMockExecutor)
        return execution_manager

    def test_get_starting_jobs(self):
        # given
        jobs = [SimpleTestJobDefinition('get_starting_jobs_01'),
                SimpleTestJobDefinition('get_starting_jobs_02'),
                SimpleTestJobDefinition('get_starting_jobs_03'),
                SimpleTestJobDefinition('get_starting_jobs_04')]
        execution_manager = self._get_execution_manager(jobs)
        build1 = execution_manager.get_build()

        build_context = {}
        for job in jobs:
            build1.add_job(job.unexpanded_id, copy.deepcopy(build_context))

        expected_starting_job_ids = [
            "get_starting_jobs_01",
            "get_starting_jobs_03"
        ]

        (build1.node
                ["get_starting_jobs_01"]
                ["object"].should_run) = True
        (build1.node
                ["get_starting_jobs_01"]
                ["object"].parents_should_run) = False
        (build1.node
                ["get_starting_jobs_02"]
                ["object"].should_run) = True
        (build1.node
                ["get_starting_jobs_02"]
                ["object"].parents_should_run) = True
        (build1.node
                ["get_starting_jobs_03"]
                ["object"].should_run) = True
        (build1.node
                ["get_starting_jobs_03"]
                ["object"].parents_should_run) = False
        (build1.node
                ["get_starting_jobs_04"]
                ["object"].should_run) = False
        (build1.node
                ["get_starting_jobs_04"]
                ["object"].parents_should_run) = False

        # when
        starting_job_ids = execution_manager.get_jobs_to_run()

        # then
        self.assertItemsEqual(starting_job_ids, expected_starting_job_ids)


    def test_no_depends_next_jobs(self):
        """tests_no_depends_next_jobs
        tests a situation where nothing depends on the job. When the job
        finishes, nothing should be returned as the next job to run
        """

        # Given
        jobs = [EffectJobDefinition("A",
            depends=None,
            targets=["A-target"])]
        execution_manager = self._get_execution_manager(jobs)


        # When
        execution_manager.running = True
        execution_manager.submit("A", {})
        execution_manager.execute("A")

        # Then
        self.assertEquals(set([]), execution_manager.get_next_jobs_to_run("A"))


    def test_simple_get_next_jobs(self):
        """test_simple_get_next_jobs
        test a situation where a job depends on a target of another job. When
        the depended on job finishes, the other job should be the next job to
        run
        """
        # Given
        jobs = [
            EffectJobDefinition("A",
                depends=None,targets=["A-target"]),
            EffectJobDefinition("B",
                depends=['A-target'], targets=["B-target"])
        ]
        execution_manager = self._get_execution_manager(jobs)

        # When
        execution_manager.running = True
        execution_manager.submit("B", {})
        execution_manager.execute("A")

        # Then
        self.assertEquals(set(["B"]), execution_manager.get_next_jobs_to_run("A"))

    def test_simple_get_next_jobs_lower(self):
        # Given
        jobs = [
            EffectJobDefinition("A", depends=None, targets=["A-target"]),
            EffectJobDefinition(
                "B", depends=["A-target"], targets=[
                    {
                        "unexpanded_id": "B-target",
                        "start_mtime": 200
                    }
                ]
            ),
            EffectJobDefinition(
                "C", depends=[
                    {
                        "unexpanded_id": "B-target",
                        "start_mtime": 200
                    }
                ], targets=["C-target"]
            )
        ]
        execution_manager = self._get_execution_manager(jobs)

        # When
        execution_manager.running = True
        execution_manager.submit("C", {})
        execution_manager.execute("A")

        # Then
        for node_id, node in execution_manager.build.node.iteritems():
            if execution_manager.build.is_target_object(node["object"]):
                print node_id, node["object"].get_mtime()
        self.assertEquals(set(["C"]), execution_manager.get_next_jobs_to_run("A"))

    def test_simple_get_next_jobs_failed_but_creates_targets(self):
        """test_simple_get_next_jobs_failed
        test a situation where a job depends on a target of another job. When
        the depended on job finishes, but fails, does not reach it's max
        fail count, and creates targets, the dependent should be next job to run
        """
        # Given
        jobs = [
            EffectJobDefinition("A",
                depends=None,targets=["A-target"], effect={"A-target": 1, "success": False}),
            EffectJobDefinition("B",
                depends=['A-target'], targets=["B-target"])
        ]
        execution_manager = self._get_execution_manager_with_effects(jobs)

        # When
        execution_manager.running = True
        execution_manager.submit("B", {})
        execution_manager.execute("A")

        # The
        self.assertEquals(set(["B"]), execution_manager.get_next_jobs_to_run("A"))

    def test_simple_get_next_jobs_failed_but_no_targets(self):
        """test_simple_get_next_jobs_failed
        test a situation where a job depends on a target of another job. When
        the depended on job finishes, but fails and does not reach it's max
        fail count, and does not create targets, the job should run again
        """
        # Given
        jobs = [
            EffectJobDefinition("A",
                depends=None,targets=["A-target"]),
            EffectJobDefinition("B",
                depends=['A-target'], targets=["B-target"])
        ]
        execution_manager = self._get_execution_manager(jobs)
        execution_manager.executor.execute = mock.Mock(return_value=mock.Mock(status=True, stdout='', stderr=''))

        # When
        execution_manager.running = True
        execution_manager.submit("B", {})
        execution_manager.execute("A")

        # The
        self.assertEquals(set(["A"]), execution_manager.get_next_jobs_to_run("A"))

    def test_simple_get_next_jobs_failed_max(self):
        """test_simple_get_next_jobs_failed_max
        test a situation where a job depends on a target of another job.
        When the depended on job finishes, but fails and reaches it's max fail
        count, return nothing as the next job to run.
        """
        # Given
        jobs = [
            EffectJobDefinition("A",
                depends=None,targets=["A-target"], effect={"A-target": None}),
            EffectJobDefinition("B",
                depends=['A-target'], targets=["B-target"])
        ]
        execution_manager = self._get_execution_manager(jobs)

        # When
        execution_manager.running = True
        execution_manager.submit("B", {})
        for i in xrange(6):
            execution_manager.execute("A")

        # The
        self.assertEquals(set([]), execution_manager.get_next_jobs_to_run("A"))

    def test_multiple_get_next_jobs(self):
        """test_multiple_get_next_jobs
        test a situation where a job creates multiple targets where individual
        jobs depend on individual targets. When the depended on job finishes, all
        of the lower jobs should be the next job to run.
        """
        # Given
        jobs = [
            EffectJobDefinition("A",
                depends=None,targets=["A-target"]),
            EffectJobDefinition("B",
                depends=['A-target'], targets=["B-target"]),
            EffectJobDefinition("C",
                depends=['A-target'], targets=["C-target"]),
            EffectJobDefinition("D",
                depends=['A-target'], targets=["D-target"]),
        ]
        execution_manager = self._get_execution_manager(jobs)

        # When
        execution_manager.running = True
        execution_manager.submit("A", {}, direction={"up", "down"})
        execution_manager.execute("A")

        # Then
        self.assertEquals({"B", "C", "D"}, set(execution_manager.get_next_jobs_to_run("A")))

    def test_multiple_get_next_jobs_failed(self):
        """test_multiple_get_next_jobs_failed
        test a situation where a job creates multiple targets where individual
        jobs depend on individual targets. When the depended on job finishes,
        but fails and does not reach it's max fail count, either the failed job
        should be the next job to run or nothing should be the next job to run.
        When the depended on job finishes it should make some of it's targets.
        This tests to make sure that when the job fails, the job's that now have
        their dependencies don't run. This is not covered by should run as there
        is a possibility that the lower nodes are check for should run before
        the parent job is invalidated.
        """
        # Given
        jobs = [
            SimpleTestJobDefinition("A",
                depends=None,targets=["A1-target", "A2-target", "A3-target"]),
            SimpleTestJobDefinition("B",
                depends=['A1-target'], targets=["B-target"]),
            SimpleTestJobDefinition("C",
                depends=['A2-target'], targets=["C-target"]),
            SimpleTestJobDefinition("D",
                depends=['A3-target'], targets=["D-target"]),
        ]
        execution_manager = self._get_execution_manager(jobs)
        execution_manager.executor.execute = mock.Mock(return_value=mock.Mock(status=True, stdout='', stderr=''))
        execution_manager.running = True
        execution_manager.submit("A", {}, direction={"up", "down"})
        execution_manager.build.get_target('A1-target').do_get_mtime = mock.Mock(return_value=None)

        # When
        execution_manager.execute("A")

        # Then
        self.assertEquals({"A"}, set(execution_manager.get_next_jobs_to_run("A")))

    def test_multiple_get_next_jobs_failed_max(self):
        """test_multiple_get_next_jobs_failed_max
        test a situation where a job creates multiple targets where individual
        jobs depend on individual targets. When the depended on job finishes,
        but fails, reaches it's max fail count, and some targets are created,
        all the jobs below with dependencies that exist should be the next jobs
        to run.
        """
        # Given
        jobs = [
            EffectJobDefinition("A",
                depends=None,targets=["A1-target", "A2-target", "A3-target"], effect={"A1-target": None, "A2-target":1, "A3-target":1}),
            EffectJobDefinition("B",
                depends=['A1-target'], targets=["B-target"]),
            EffectJobDefinition("C",
                depends=['A2-target'], targets=["C-target"]),
            EffectJobDefinition("D",
                depends=['A3-target'], targets=["D-target"]),
        ]
        execution_manager = self._get_execution_manager(jobs)
        execution_manager.running = True
        execution_manager.submit("A", {}, direction={"up", "down"})

        # When
        for i in xrange(6):
            execution_manager.execute("A")
        execution_manager.build.get_target('A1-target').do_get_mtime = mock.Mock(return_value=None)

        # Then
        self.assertEquals({"C", "D"}, set(execution_manager.get_next_jobs_to_run("A")))
        self.assertEquals(execution_manager.get_build().get_job("C").get_should_run(), True)
        self.assertEquals(execution_manager.get_build().get_job("D").get_should_run(), True)

    def test_depends_one_or_more_next_jobs(self):
        """test_depends_one_or_more_next_jobs
        test a situation where a job has a depends one or more dependency. It is
        not past it's curfew so it needs all of the dependencies to run.
        Complete each of it's dependencies individually. Each one should return
        nothing until the last one.
        """
        # Given
        jobs = [
            EffectJobDefinition("A1",
                depends=None, targets=["A1-target"],
                effect={"A1-target": 1}),
            EffectJobDefinition("A2",
                depends=None, targets=["A2-target"],
                effect=[{"A2-target": None}, {"A2-target": 1}]),
            EffectJobDefinition("B",
                depends=[
                    {"unexpanded_id": "A1-target", "type": "depends_one_or_more"},
                    {"unexpanded_id": "A2-target", "type": "depends_one_or_more"}],
                targets=["B-target"])
        ]
        execution_manager = self._get_execution_manager_with_effects(jobs)
        build_context = {"start_time": arrow.get("2015-01-01-00-00"), "end_time": arrow.get("2015-01-01-00-10")}
        execution_manager.running = True
        execution_manager.submit("B", build_context)

        # When
        execution_manager.execute("A1")
        execution_manager.execute("A2")

        # Then
        self.assertEquals(set(), set(execution_manager.get_next_jobs_to_run("A1")))
        self.assertEquals({"A2"}, set(execution_manager.get_next_jobs_to_run("A2")))
        self.assertEquals(execution_manager.get_build().get_job("B").get_should_run(), False)

        # On rerun, A2 complete successfully and therefore B should run
        execution_manager.execute("A2")
        self.assertEquals({"B"}, set(execution_manager.get_next_jobs_to_run("A1")))
        self.assertEquals({"B"}, set(execution_manager.get_next_jobs_to_run("A2")))
        self.assertEquals(execution_manager.get_build().get_job("B").get_should_run(), True)


    def test_depends_one_or_more_next_jobs_failed_max_lower(self):
        """test_depends_one_or_more_next_jobs_failed
        test a situation where a job has a depends one or more dependency. It
        is not past it's curfew so it needs all of the dependencies to run.
        Each of the dependencies should also depend on a single job so there are
        a total of three layers of jobs. Complete each of the jobs in the first
        two rows except the last job. The last job in the first row should fail
        and reach it's max fail count. It's next job should be the job in the
        bottom row as all of it's buildable dependencies are built and all of
        the non buildable dependencies are due to a failure.
        """
        jobs = [
            EffectTimestampExpandedJobDefinition("A", file_step="5min",
                depends=None,
                targets=[{"unexpanded_id": "A-target-%Y-%m-%d-%H-%M", "file_step": "5min"}]),
            EffectTimestampExpandedJobDefinition("B", file_step="5min",
                depends=None,
                targets=[{"unexpanded_id": "B-target-%Y-%m-%d-%H-%M", "file_step": "5min"}],
                effect=[{"B-target-2015-01-01-00-00": 1, "B-target-2015-01-01-00-05": None, "success": False}]),
            EffectJobDefinition("C", expander_type=TimestampExpander,
                depends=[
                    {"unexpanded_id": "A-target-%Y-%m-%d-%H-%M", "file_step": "5min", "type": "depends_one_or_more"},
                    {"unexpanded_id": "B-target-%Y-%m-%d-%H-%M", "file_step": "5min", "type": "depends_one_or_more"}],
                targets=[{"unexpanded_id": "C-target-%Y-%m-%d-%H-%M", "file_step": "5min"}])
        ]
        execution_manager = self._get_execution_manager_with_effects(jobs)
        build_context = {"start_time": arrow.get("2015-01-01-00-00"), "end_time": arrow.get("2015-01-01-00-10")}
        execution_manager.running = True
        execution_manager.submit("C", build_context)

        # When
        executions = ["A_2015-01-01-00-05-00", "A_2015-01-01-00-00-00", "B_2015-01-01-00-00-00"] + ["B_2015-01-01-00-05-00"]*6
        for execution in executions:
            execution_manager.execute(execution)

        # Then
        self.assertEquals(execution_manager.get_build().get_job("B_2015-01-01-00-05-00").get_should_run(), False)
        self.assertEquals(execution_manager.get_build().get_job("B_2015-01-01-00-05-00").get_stale(), True)
        for job_id in ("A_2015-01-01-00-05-00", "A_2015-01-01-00-00-00", "B_2015-01-01-00-00-00", "B_2015-01-01-00-05-00"):
            self.assertEquals({"C"}, set(execution_manager.get_next_jobs_to_run(job_id)))
        self.assertEquals(execution_manager.get_build().get_job("C").get_should_run(), True)


    def test_upper_update(self):
        """tests situations where a job starts running and while it is running a
        job above it should run again, possibly due to a target being deleted
        or a force. When the running job finishes, none of it's lower jobs
        should run.
        """
        # Given
        jobs = [
            EffectJobDefinition("A",
                depends=None, targets=["A-target"],
                effect=[1,100]),
            EffectJobDefinition("B",
                depends=["A-target"], targets=["B-target"], effect=1),
            EffectJobDefinition("C",
                depends=["B-target"], targets=["C-target"], effect=1),
        ]
        execution_manager = self._get_execution_manager_with_effects(jobs)
        build_context = {}
        execution_manager.running = True
        execution_manager.submit("C", build_context)

        # When
        execution_manager.execute("A")
        execution_manager.execute("B")
        execution_manager.submit("A", {}, force=True)
        for node_id, node in execution_manager.build.node.iteritems():
            if execution_manager.build.is_target(node_id):
                print node_id, node["object"].get_mtime()

        # Then
        self.assertEquals(set(), set(execution_manager.get_next_jobs_to_run("B")))

    def test_multiple_targets_one_exists(self):
        # Given
        jobs = [
            EffectJobDefinition("A",
                depends=None, targets=["A1-target", "A2-target"],
                effect=[{"A1-target": 1, "A2-target": None}, {"A1-target": 1, "A2-target": None}, {"A1-target": 1, "A2-target": 4}]),
            EffectJobDefinition("B",
                depends=["A1-target"], targets=["B-target"], effect=2),
            EffectJobDefinition("C",
                depends=["A2-target"], targets=["C-target"], effect=5),
            EffectJobDefinition("D",
                depends=["B-target"], targets=["D-target"], effect=3),
            EffectJobDefinition("E",
                depends=["C-target"], targets=["E-target"], effect=6),
        ]
        execution_manager = self._get_execution_manager_with_effects(jobs)
        build_context = {}
        execution_manager.build.add_job("A", build_context, direction={"down", "up"})

        # When
        for execution in ("A", "B", "C", "E", "A", "A"):
            job = execution_manager.build.get_job(execution)
            if job.get_should_run_immediate():
                print execution
                execution_manager.execute(execution)

        # Then
        self.assertEquals({"C", "D"}, set(execution_manager.get_next_jobs_to_run("A")))
        self.assertEquals(execution_manager.get_build().get_job("C").get_should_run(), True)
        execution_manager.execute("C")
        self.assertEquals({"E"}, set(execution_manager.get_next_jobs_to_run("C")))
        self.assertEquals(execution_manager.get_build().get_job("E").get_should_run(), True)
        self.assertEquals(execution_manager.get_build().get_job("D").get_should_run(), True)

    def test_effect_job(self):
        # Given
        jobs = [
            EffectJobDefinition("A", targets=["A-target"]),
            EffectJobDefinition("B", depends=["A-target"], targets=["B-target"]),
        ]
        execution_manager = self._get_execution_manager_with_effects(jobs)

        # When
        execution_manager.running = True
        execution_manager.submit("B", {})
        execution_manager.start_execution(inline=True)

        # Then
        job_A = execution_manager.build.get_job("A")
        job_B = execution_manager.build.get_job("B")

        self.assertEqual(job_A.count, 1)
        self.assertEqual(job_B.count, 1)


class ExecutionDaemonTests(unittest.TestCase):
    def _get_execution_manager_with_effects(self):
        build_manager = BuildManager([EffectTimestampExpandedJobDefinition("A", file_step="5min",
            targets=[{"unexpanded_id": "A-target", "file_step": "5min"}])], metas=[])
        execution_manager = ExecutionManager(build_manager, ExtendedMockExecutor)
        return execution_manager

    def test_submission(self):
        # Given
        execution_manager = self._get_execution_manager_with_effects()
        json_body = json.dumps({
          "job_definition_id": "A",
          "build_context": {
            "start_time": "2015-04-01", "end_time": "2015-04-01"
          }
        })

        # When
        execution_manager.running = True
        _submit_from_json(execution_manager, json_body)

        # Then
        self.assertTrue(execution_manager.get_build().is_job("A_2015-04-01-00-00-00"))

    def test_update_lower_nodes(self):
        """test_update_lower_nodes
        Add a node that should run that is above the nodes in the graph.
        All the other nodes originally didn't have parent's that should run.
        Now they do.
        """
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1']),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1']),
            SimpleTestJobDefinition('job3', targets=['target3'],
                                    depends=['target2']),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        execution_manager.build.add_job('job1', {}, depth=1)
        execution_manager.build.add_job('job2', {}, depth=1)
        execution_manager.build.add_job('job3', {}, depth=1)

        build_graph = execution_manager.build
        job1 = build_graph.get_job('job1')
        job2 = build_graph.get_job('job2')
        job3 = build_graph.get_job('job3')
        job1.get_should_run_immediate = mock.Mock(return_value=True)
        job2.parents_should_run = False
        job3.parents_should_run = False

        # When
        execution_manager.update_parents_should_run('job1')

        # Then
        self.assertFalse(job1.parents_should_run)
        self.assertIsNone(job2.parents_should_run)
        self.assertIsNone(job3.parents_should_run)

    def test_update_lower_nodes_connection(self):
        """test_update_lower_nodes_connection
        Add a node that shouldn't run but has parent's that should run.
        This node connects the parent nodes to lower nodes.
        All the lower nodes originally didn't have parent's that should run.
        Now they do.
        """
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1']),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1']),
            SimpleTestJobDefinition('job3', targets=['target3'],
                                    depends=['target2']),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        execution_manager.build.add_job('job1', {}, depth=1)
        execution_manager.build.add_job('job2', {}, depth=1)
        execution_manager.build.add_job('job3', {}, depth=1)

        build_graph = execution_manager.build
        job1 = build_graph.get_job('job1')
        job2 = build_graph.get_job('job2')
        job3 = build_graph.get_job('job3')
        job1.get_should_run_immediate = mock.Mock(return_value=True)
        job2.get_should_run_immediate = mock.Mock(return_value=False)
        job3.parents_should_run = False

        # When
        execution_manager.update_parents_should_run('job2')

        # Then
        self.assertTrue(job2.parents_should_run)
        self.assertIsNone(job3.parents_should_run)

    def test_update_lower_nodes_cached(self):
        """test_update_lower_nodes_cached
        Add a node that should run. Update all lower nodes until you get to a
        node that already had parent's that should have ran.
        """
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1']),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1']),
            SimpleTestJobDefinition('job3', targets=['target3'],
                                    depends=['target2']),
            SimpleTestJobDefinition('job4', targets=['target4'],
                                    depends=['target3']),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        execution_manager.build.add_job('job1', {}, depth=1)
        execution_manager.build.add_job('job2', {}, depth=1)
        execution_manager.build.add_job('job3', {}, depth=1)
        execution_manager.build.add_job('job4', {}, depth=1)

        build_graph = execution_manager.build
        job1 = build_graph.get_job('job1')
        job2 = build_graph.get_job('job2')
        job3 = build_graph.get_job('job3')
        job4 = build_graph.get_job('job4')
        job1.get_should_run_immediate = mock.Mock(return_value=True)
        job2.parents_should_run = False
        job3.parents_should_run = True
        job4.parents_should_run = False # can't really happen
                                        # just being used to check stopage

        # When
        execution_manager.update_parents_should_run('job1')

        # Then
        self.assertIsNone(job2.parents_should_run)
        self.assertTrue(job3.parents_should_run)
        self.assertFalse(job4.parents_should_run)

    def test_update_lower_exit_early(self):
        """test_update_lower_exit_early
        Add a node that should not run and it's parent's should not run.
        Don't invalidate the lower nodes
        """
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1']),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1']),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        execution_manager.build.add_job('job1', {}, depth=1)
        execution_manager.build.add_job('job2', {}, depth=1)

        build_graph = execution_manager.build
        job1 = build_graph.get_job('job1')
        job2 = build_graph.get_job('job2')
        job1.get_should_run_immediate = mock.Mock(return_value=False)
        job2.parents_should_run = False

        # When
        execution_manager.update_parents_should_run('job1')

        # Then
        self.assertFalse(job2.parents_should_run)

    def test_update_lower_nodes_ignore_parents(self):
        """test_update_lower_nodes_ignore_parents
        Add a node that should run. Update all lower nodes until you get to a
        node that has a node that ignores it's parents
        """
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1']),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1']),
            SimpleTestJobDefinition('job3', targets=['target3'],
                                    depends=['target2']),
            SimpleTestJobDefinition('job4', targets=['target4'],
                                    depends=['target3']),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        execution_manager.build.add_job('job1', {}, depth=1)
        execution_manager.build.add_job('job2', {}, depth=1)
        execution_manager.build.add_job('job3', {}, depth=1)
        execution_manager.build.add_job('job4', {}, depth=1)

        build_graph = execution_manager.build
        job1 = build_graph.get_job('job1')
        job2 = build_graph.get_job('job2')
        job3 = build_graph.get_job('job3')
        job4 = build_graph.get_job('job4')
        job1.get_should_run_immediate = mock.Mock(return_value=True)
        job2.parents_should_run = False
        job3.ignore_parents = mock.Mock(return_value=True)
        job3.parents_should_run = False
        job4.parents_should_run = False # can't really happen
                                        # just being used to check stopage

        # When
        execution_manager.update_parents_should_run('job1')

        # Then
        self.assertIsNone(job2.parents_should_run)
        self.assertFalse(job3.parents_should_run)
        self.assertFalse(job4.parents_should_run)

    def test_double_update_lower_nodes(self):
        """test_double_update_lower_nodes
        Update two nodes, make sure that all the things get iterated
        through
        """
        # Given
        jobs = [
            SimpleTestJobDefinition("job1", targets=["target1"]),
            SimpleTestJobDefinition("job2", targets=["target2"],
                                    depends=["target1"]),
            SimpleTestJobDefinition("job3", targets=["target3"],
                                    depends=["target2"]),
            SimpleTestJobDefinition("job1'", targets=["target1'"]),
            SimpleTestJobDefinition("job2'", targets=["target2'"],
                                    depends=["target1'"]),
            SimpleTestJobDefinition("job3'", targets=["target3'"],
                                    depends=["target2'"]),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        execution_manager.build.add_job("job1", {}, depth=1)
        execution_manager.build.add_job("job2", {}, depth=1)
        execution_manager.build.add_job("job3", {}, depth=1)
        execution_manager.build.add_job("job1'", {}, depth=1)
        execution_manager.build.add_job("job2'", {}, depth=1)
        execution_manager.build.add_job("job3'", {}, depth=1)

        build_graph = execution_manager.build
        job1 = build_graph.get_job("job1")
        job2 = build_graph.get_job("job2")
        job3 = build_graph.get_job("job3")
        job1_ = build_graph.get_job("job1'")
        job2_ = build_graph.get_job("job2'")
        job3_ = build_graph.get_job("job3'")
        job1.get_should_run_immediate = mock.Mock(return_value=True)
        job2.parents_should_run = False
        job3.parents_should_run = False
        job1_.get_should_run_immediate = mock.Mock(return_value=True)
        job2_.parents_should_run = False
        job3_.parents_should_run = False

        # When
        for job_id in ["job1", "job1'"]:
            execution_manager.update_parents_should_run(job_id)

        # Then
        self.assertFalse(job1.parents_should_run)
        self.assertIsNone(job2.parents_should_run)
        self.assertIsNone(job3.parents_should_run)
        self.assertFalse(job1_.parents_should_run)
        self.assertIsNone(job2_.parents_should_run)
        self.assertIsNone(job3_.parents_should_run)

    def test_update_lower_first_ignores_parents(self):
        """test_update_lower_first_ignores_parents
        A test where the first job ignores it's parents. Nothing should be
        updated as everything ignores things that ignores it's parents
        """
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1']),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1']),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        execution_manager.build.add_job('job1', {}, depth=1)
        execution_manager.build.add_job('job2', {}, depth=1)

        build_graph = execution_manager.build
        job1 = build_graph.get_job('job1')
        job2 = build_graph.get_job('job2')
        job1.get_should_run_immediate = mock.Mock(return_value=True)
        job2.parents_should_run = False

        # When
        execution_manager.update_parents_should_run('job1')

        # Then
        self.assertFalse(job2.parents_should_run)

    def test_update_lower_same_twice(self):
        """test_update_lower_same_twice
        A test that has two jobs updated that have the same dependant job
        The first job should invalidate the job, the second should find that
        it's parents should run and stop there
        """
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1']),
            SimpleTestJobDefinition('job2', targets=['target2']),
            SimpleTestJobDefinition('job3', targets=['target3'],
                                    depends=[
                                        'target1',
                                        'target2',
                                    ]),
            SimpleTestJobDefinition('job4', targets=['target4'],
                                    depends=['target3']),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        execution_manager.build.add_job('job1', {}, depth=1)
        execution_manager.build.add_job('job2', {}, depth=1)
        execution_manager.build.add_job('job3', {}, depth=1)
        execution_manager.build.add_job('job4', {}, depth=1)

        build_graph = execution_manager.build
        job1 = build_graph.get_job('job1')
        job2 = build_graph.get_job('job2')
        job3 = build_graph.get_job('job3')
        job4 = build_graph.get_job('job4')
        job1.get_should_run_immediate = mock.Mock(return_value=True)
        job2.get_should_run_immediate = mock.Mock(return_value=True)
        job3.parents_should_run = False
        job4.parents_should_run = False

        # When
        for job_id in ["job1", "job2"]:
            execution_manager.update_parents_should_run(job_id)

        # Then
        self.assertTrue(job3.parents_should_run)
        self.assertIsNone(job4.parents_should_run)

    def test_addition_updates(self):
        """test_addition_updates
        This tests to make sure that when a job is added to the graph that the
        execution system updates the graph to be consisitent
        """
        jobs = [
            SimpleTestJobDefinition(
                'job1', targets=[
                    {
                        'unexpanded_id': 'target1',
                        'start_mtime': 100,
                    }
                ], depends=[
                    {
                        'unexpanded_id': 'super_target1',
                        'start_mtime': 200,
                    }
                ]
            ),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1']),
            SimpleTestJobDefinition('job3', targets=['target3'],
                                    depends=['target2']),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        execution_manager.build.add_job('job2', {}, depth=1)
        execution_manager.build.add_job('job3', {}, depth=1)

        build_graph = execution_manager.build
        job2 = build_graph.get_job('job2')
        job3 = build_graph.get_job('job3')
        job2.parents_should_run = False
        job3.parents_should_run = False

        # When
        execution_manager.running = True
        execution_manager.submit('job1', {})
        job1 = build_graph.get_job('job1')

        # Then
        self.assertFalse(job1.parents_should_run)
        self.assertIsNone(job2.parents_should_run)
        self.assertIsNone(job3.parents_should_run)

    def test_update_target_no_creator_should_not_run(self):
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1'],
                                    depends=['super_target1']),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1'])
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        build_graph = execution_manager.build
        build_graph.add_job("job2", {})

        job1 = build_graph.get_job("job1")
        job2 = build_graph.get_job("job2")

        job1.should_run_immediate = True
        job2.should_run_immediate = True
        job1.parents_should_run = False
        job2.parents_should_run = True

        super_target1 = build_graph.get_target("super_target1")
        target1 = build_graph.get_target("target1")
        target2 = build_graph.get_target("target2")
        super_target1.do_get_mtime = mock.Mock(return_value=100)
        target1.do_get_mtime = mock.Mock(return_value=200)
        target2.do_get_mtime = mock.Mock(return_value=None)

        # When
        execution_manager.external_update_targets(['super_target1'])

        # Then
        self.assertFalse(execution_manager._work_queue.empty())
        self.assertEqual(execution_manager._work_queue.get(False), 'job2')
        self.assertTrue(execution_manager._work_queue.empty())

    def test_update_target_no_creator_should_run(self):
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1'],
                                    depends=['super_target1']),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1'])
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        build_graph = execution_manager.build
        build_graph.add_job("job2", {})

        job1 = build_graph.get_job("job1")
        job2 = build_graph.get_job("job2")

        job1.should_run_immediate = True
        job2.should_run_immediate = True
        job1.parents_should_run = False
        job2.parents_should_run = True

        super_target1 = build_graph.get_target("super_target1")
        target1 = build_graph.get_target("target1")
        target2 = build_graph.get_target("target2")
        super_target1.do_get_mtime = mock.Mock(return_value=100)
        target1.do_get_mtime = mock.Mock(return_value=50)
        target2.do_get_mtime = mock.Mock(return_value=None)

        # When
        execution_manager.external_update_targets(['super_target1'])

        # Then
        self.assertFalse(execution_manager._work_queue.empty())
        self.assertEqual(execution_manager._work_queue.get(False), 'job1')
        self.assertTrue(execution_manager._work_queue.empty())

    def test_update_target_no_creator_should_not_run_deep(self):
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1'],
                                    depends=['super_target1']),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1']),
            SimpleTestJobDefinition('job3', targets=['target3'],
                                    depends=['target2']),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        build_graph = execution_manager.build
        build_graph.add_job("job3", {})

        job1 = build_graph.get_job("job1")
        job2 = build_graph.get_job("job2")
        job3 = build_graph.get_job("job3")

        job1.should_run_immediate = True
        job2.should_run_immediate = False
        job3.should_run_immediate = True
        job1.parents_should_run = False
        job2.parents_should_run = True
        job3.parents_should_run = True

        super_target1 = build_graph.get_target("super_target1")
        target1 = build_graph.get_target("target1")
        target2 = build_graph.get_target("target2")
        target3 = build_graph.get_target("target3")
        super_target1.do_get_mtime = mock.Mock(return_value=100)
        target1.do_get_mtime = mock.Mock(return_value=150)
        target2.do_get_mtime = mock.Mock(return_value=200)
        target3.do_get_mtime = mock.Mock(return_value=None)

        # When
        execution_manager.external_update_targets(['super_target1'])

        # Then
        self.assertFalse(execution_manager._work_queue.empty())
        self.assertEqual(execution_manager._work_queue.get(False), 'job3')
        self.assertTrue(execution_manager._work_queue.empty())

    def test_update_target_should_run(self):
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1']),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1']),
            SimpleTestJobDefinition('job3', targets=['target3'],
                                    depends=['target2']),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        build_graph = execution_manager.build
        build_graph.add_job("job3", {})

        job1 = build_graph.get_job("job1")
        job2 = build_graph.get_job("job2")
        job3 = build_graph.get_job("job3")

        job1.should_run_immediate = False
        job2.should_run_immediate = False
        job3.should_run_immediate = True
        job1.parents_should_run = False
        job2.parents_should_run = False
        job3.parents_should_run = False

        target1 = build_graph.get_target("target1")
        target2 = build_graph.get_target("target2")
        target3 = build_graph.get_target("target3")
        target1.do_get_mtime = mock.Mock(return_value=None)
        target2.do_get_mtime = mock.Mock(return_value=100)
        target3.do_get_mtime = mock.Mock(return_value=50)

        # When
        execution_manager.external_update_targets(['target1'])

        # Then
        self.assertFalse(execution_manager._work_queue.empty())
        self.assertEqual(execution_manager._work_queue.get(False), 'job1')
        self.assertTrue(job3.get_parents_should_run())
        self.assertTrue(execution_manager._work_queue.empty())

    def test_update_target_multiple(self):
        # Given
        jobs = [
            SimpleTestJobDefinition('job1', targets=['target1']),
            SimpleTestJobDefinition('job2', targets=['target2'],
                                    depends=['target1']),
            SimpleTestJobDefinition('job3', targets=['target3'],
                                    depends=['target2']),
            SimpleTestJobDefinition("job1'", targets=["target1'"],
                                    depends=["super_target1'"]),
            SimpleTestJobDefinition("job2'", targets=["target2'"],
                                    depends=["target1'"]),
            SimpleTestJobDefinition("job3'", targets=["target3'"],
                                    depends=["target2'"]),
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)

        build_graph = execution_manager.build
        build_graph.add_job("job3", {})
        build_graph.add_job("job3'", {})

        job1 = build_graph.get_job("job1")
        job2 = build_graph.get_job("job2")
        job3 = build_graph.get_job("job3")
        job1_ = build_graph.get_job("job1'")
        job2_ = build_graph.get_job("job2'")
        job3_ = build_graph.get_job("job3'")

        job1.should_run_immediate = False
        job2.should_run_immediate = False
        job3.should_run_immediate = True
        job1.parents_should_run = False
        job2.parents_should_run = False
        job3.parents_should_run = False
        job1_.should_run_immediate = True
        job2_.should_run_immediate = False
        job3_.should_run_immediate = True
        job1_.parents_should_run = False
        job2_.parents_should_run = True
        job3_.parents_should_run = True

        target1 = build_graph.get_target("target1")
        target2 = build_graph.get_target("target2")
        target3 = build_graph.get_target("target3")
        target1.do_get_mtime = mock.Mock(return_value=None)
        target2.do_get_mtime = mock.Mock(return_value=100)
        target3.do_get_mtime = mock.Mock(return_value=50)

        super_target1_ = build_graph.get_target("super_target1'")
        target1_ = build_graph.get_target("target1'")
        target2_ = build_graph.get_target("target2'")
        target3_ = build_graph.get_target("target3'")
        super_target1_.do_get_mtime = mock.Mock(return_value=100)
        target1_.do_get_mtime = mock.Mock(return_value=150)
        target2_.do_get_mtime = mock.Mock(return_value=200)
        target3_.do_get_mtime = mock.Mock(return_value=None)

        # When
        execution_manager.external_update_targets(["target1", "super_target1'"])

        # Then
        self.assertFalse(execution_manager._work_queue.empty())
        work_job1 = execution_manager._work_queue.get(False)
        self.assertFalse(execution_manager._work_queue.empty())
        work_job2 = execution_manager._work_queue.get(False)
        self.assertIn(work_job1, ["job3'", "job1"])
        self.assertIn(work_job2, ["job3'", "job1"])
        self.assertNotEqual(work_job1, work_job2)
        self.assertTrue(job3.get_parents_should_run())
        self.assertTrue(execution_manager._work_queue.empty())

    def test_force_existing(self):
        # Given
        jobs = [
            EffectJobDefinition(
                "job1", depends=[
                    {
                        "unexpanded_id": "super_target1",
                        "start_mtime": 100,
                    }
                ], targets=["target1"], effect=200
            )
        ]

        build_manager = BuildManager(jobs, [])
        execution_manager = ExecutionManager(build_manager,
                                             ExtendedMockExecutor)
        execution_manager.executor.execute = mock.Mock(wraps=execution_manager.executor.execute)

        execution_manager.running = True
        execution_manager.submit("job1", {})
        execution_manager.start_execution(inline=True)

        # When
        execution_manager.running = True
        execution_manager.submit("job1", {})
        self.assertEqual(execution_manager._work_queue.qsize(), 0)
        execution_manager.start_execution(inline=True)

        execution_manager.running = True
        execution_manager.submit("job1", {}, force=True)
        self.assertEqual(execution_manager._work_queue.qsize(), 1)
        execution_manager.start_execution(inline=True)

        # Then
        self.assertEqual(execution_manager.executor.execute.call_count, 2)
