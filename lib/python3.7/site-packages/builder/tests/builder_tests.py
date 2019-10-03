"""Used to test the construction of graphs and general use of graphs"""

import copy
import unittest
import datetime

import dateutil
import arrow
import mock
import networkx

from builder.tests.tests_jobs import *
import builder.jobs
import builder.build
import builder.util
import builder.targets


class GraphTest(unittest.TestCase):
    """Used to test the general graph construction"""

    def test_expand_10s(self):
        # Given
        build_manager = builder.build.BuildManager(
            [SimpleTimestampExpandedTestJob("test_second_job", file_step="10s")], [])
        build = build_manager.make_build()

        # When
        build.add_job("test_second_job", {'start_time': arrow.get("2015-01-01T00:00:00+00:00"),
                                     'end_time': arrow.get("2015-01-01T00:01:00+00:00")})

        # Then
        self.assertEquals(len(build.node), 6)

    def test_rule_dep_construction(self):
        # Given
        jobs = [
            SimpleTestJobDefinition("rule_dep_construction_job_top_01",
                expander_type=builder.expanders.TimestampExpander,
                depends=[{"unexpanded_id": "rule_dep_construction_target_highest_01", "file_step": "5min"}],
                targets=[{"unexpanded_id": "rule_dep_construction_top_01", "file_step": "5min"}]
            ),
            SimpleTestJobDefinition("rule_dep_construction_job_top_02",
                expander_type=builder.expanders.TimestampExpander,
                depends=[
                    {"unexpanded_id": "rule_dep_construction_target_highest_02", "file_step": "5min"},
                    {"unexpanded_id": "rule_dep_construction_target_highest_03", "file_step": "5min"},
                    {"unexpanded_id": "rule_dep_construction_target_highest_04", "file_step": "5min"},
                    {"unexpanded_id": "rule_dep_construction_target_highest_04", "file_step": "5min", "type": "depends_one_or_more"}
                ],
                targets=[
                    {"unexpanded_id": "rule_dep_construction_target_top_02", "file_step": "5min"},
                    {"unexpanded_id": "rule_dep_construction_target_top_03", "file_step": "5min"},
                    {"unexpanded_id": "rule_dep_construction_target_top_04", "file_step": "5min", "type": "alternates"}
                ]),
            SimpleTestJobDefinition("rule_dep_construction_job_bottom_01",
                expander_type=builder.expanders.TimestampExpander,
                depends=[{"unexpanded_id": "rule_dep_construction_target_top_02", "file_step": "5min"},
                    {"unexpanded_id": "rule_dep_construction_target_top_03", "file_step": "5min", "past": 3},
                    {"unexpanded_id": "rule_dep_construction_target_top_04", "file_step": "5min", "type": "depends_one_or_more"}],
                targets=[
                    {"unexpanded_id": "rule_dep_construction_target_bottom_01", "file_step": "5min"},
                ])
        ]

        expected_edges = (
            ("rule_dep_construction_target_highest_02",
             "rule_dep_construction_job_top_02",
             {"label": "depends"}),
            ("rule_dep_construction_target_highest_02",
             "rule_dep_construction_job_top_02",
             {"label": "depends"}),
            ("rule_dep_construction_target_highest_03",
             "rule_dep_construction_job_top_02",
             {"label": "depends"}),
            ("rule_dep_construction_target_highest_04",
             "rule_dep_construction_job_top_02",
             {"label": "depends_one_or_more"}),
            ("rule_dep_construction_job_top_02",
             "rule_dep_construction_target_top_02",
             {"label": "produces"}),
            ("rule_dep_construction_job_top_02",
             "rule_dep_construction_target_top_03",
             {"label": "produces"}),
            ("rule_dep_construction_job_top_02",
             "rule_dep_construction_target_top_04",
             {"label": "alternates"}),
            ("rule_dep_construction_target_top_02",
             "rule_dep_construction_job_bottom_01",
             {"label": "depends"}),
            ("rule_dep_construction_target_top_03",
             "rule_dep_construction_job_bottom_01",
             {"label": "depends"}),
            ("rule_dep_construction_target_top_04",
             "rule_dep_construction_job_bottom_01",
             {"label": "depends_one_or_more"}),
            ("rule_dep_construction_job_bottom_01",
             "rule_dep_construction_target_bottom_01",
             {"label": "produces"}),
        )

        build_manager = builder.build.BuildManager(jobs, [])
        graph = build_manager.make_build()

        # When
        rule_dependency_graph = graph.rule_dependency_graph

        # Then
        for expected_edge in expected_edges:
            for actual_edge in rule_dependency_graph.edges_iter(data=True):
                if expected_edge == actual_edge:
                    break
            else:
                self.assertTrue(
                        False,
                        msg="{} is not in the graph".format(expected_edge))

    def test_build_plan_construction(self):
        # Given
        jobs = [
            SimpleTimestampExpandedTestJob("build_graph_construction_job_top_01", file_step="5min",
                expander_type=builder.expanders.TimestampExpander,
                target_type=builder.targets.LocalFileSystemTarget,
                depends=[{"unexpanded_id": "build_graph_construction_target_highest_01-%Y-%m-%d-%H-%M", "file_step": "1min"}],
                targets=[{"unexpanded_id": "build_graph_construction_target_top_01-%Y-%m-%d-%H-%M", "file_step": "5min"}]
            ),
            SimpleTimestampExpandedTestJob("build_graph_construction_job_top_02", file_step="5min",
                expander_type=builder.expanders.TimestampExpander,
                targets=[
                    {"unexpanded_id": "build_graph_construction_target_top_02-%Y-%m-%d-%H-%M", "file_step": "1min"},
                    {"unexpanded_id": "build_graph_construction_target_top_03-%Y-%m-%d-%H-%M", "file_step": "5min"},
                    {"unexpanded_id": "build_graph_construction_target_top_04-%Y-%m-%d-%H-%M", "file_step": "5min", "type": "alternates"}],
                depends=[
                    {"unexpanded_id": "build_graph_construction_target_highest_02-%Y-%m-%d-%H-%M", "file_step": "5min"},
                    {"unexpanded_id": "build_graph_construction_target_highest_03-%Y-%m-%d-%H-%M", "file_step": "1min"},
                    {"unexpanded_id": "build_graph_construction_target_highest_04-%Y-%m-%d-%H-%M", "file_step": "1min", "type": "depends_one_or_more"}],
            ),
            SimpleTimestampExpandedTestJob("build_graph_construction_job_bottom_01", file_step="1h",
                expander_type=builder.expanders.TimestampExpander,
                targets=[{"unexpanded_id": "build_graph_construction_target_bottom_01-%Y-%m-%d-%H-%M", "file_step": "5min"}],
                depends=[{"unexpanded_id": "build_graph_construction_target_top_02-%Y-%m-%d-%H-%M", "file_step": "1min"},
                    {"unexpanded_id": "build_graph_construction_target_top_03-%Y-%m-%d-%H-%M", "file_step": "5min", "past": 3},
                    {"unexpanded_id": "build_graph_construction_target_top_04-%Y-%m-%d-%H-%M", "file_step": "5min", "type": "depends_one_or_more"}],
            )
        ]

        start_time = "2014-12-05T10:30"
        start_time = arrow.get(start_time)
        end_time = "2014-12-05T11:30"
        end_time = arrow.get(end_time)

        start_job1 = "build_graph_construction_job_bottom_01"
        start_job2 = "build_graph_construction_job_top_01"

        build_context1 = {
                "start_time": start_time,
                "end_time": end_time,
        }
        build_context2 = {
                "start_time": start_time,
                "end_time": end_time,
        }

        node1_id = "build_graph_construction_job_top_01_2014-12-05-10-30-00"
        node2_id = "build_graph_construction_job_top_01_2014-12-05-11-25-00"
        node3_id = "build_graph_construction_job_top_02_2014-12-05-10-00-00"
        node4_id = "build_graph_construction_job_top_02_2014-12-05-10-55-00"
        node5_id = "build_graph_construction_job_bottom_01_2014-12-05-10-00-00"

        expected_number_of_parents1 = 1
        expected_number_of_parents2 = 1
        expected_number_of_parents3 = 3
        expected_number_of_parents4 = 3
        expected_number_of_parents5 = 3

        expected_number_of_dependencies1 = 5
        expected_number_of_dependencies2 = 5
        expected_number_of_dependencies3 = 11
        expected_number_of_dependencies4 = 11
        expected_number_of_dependencies5 = 60 + 12 + 3 + 12

        expected_number_of_targets1 = 1
        expected_number_of_targets2 = 1
        expected_number_of_targets3 = 7
        expected_number_of_targets4 = 7
        expected_number_of_targets5 = 12

        # When
        build_manager = builder.build.BuildManager(jobs, [])
        build = build_manager.make_build()

        build.add_job(start_job1, build_context1)
        build.add_job(start_job2, build_context2)

        build_graph = build

        depends1 = build_graph.predecessors(node1_id)
        depends2 = build_graph.predecessors(node2_id)
        depends3 = build_graph.predecessors(node3_id)
        depends4 = build_graph.predecessors(node4_id)
        depends5 = build_graph.predecessors(node5_id)

        number_of_parents1 = len(depends1)
        number_of_parents2 = len(depends2)
        number_of_parents3 = len(depends3)
        number_of_parents4 = len(depends4)
        number_of_parents5 = len(depends5)

        number_of_dependencies1 = 0
        for depends in depends1:
            number_of_dependencies1 = (number_of_dependencies1 +
                    len(build_graph.predecessors(depends)))
        number_of_dependencies2 = 0
        for depends in depends2:
            number_of_dependencies2 = (number_of_dependencies2 +
                    len(build_graph.predecessors(depends)))
        number_of_dependencies3 = 0
        for depends in depends3:
            number_of_dependencies3 = (number_of_dependencies3 +
                    len(build_graph.predecessors(depends)))
        number_of_dependencies4 = 0
        for depends in depends4:
            number_of_dependencies4 = (number_of_dependencies4 +
                    len(build_graph.predecessors(depends)))
        number_of_dependencies5 = 0
        for depends in depends5:
            number_of_dependencies5 = (number_of_dependencies5 +
                    len(build_graph.predecessors(depends)))

        targets1 = build_graph.neighbors(node1_id)
        targets2 = build_graph.neighbors(node2_id)
        targets3 = build_graph.neighbors(node3_id)
        targets4 = build_graph.neighbors(node4_id)
        targets5 = build_graph.neighbors(node5_id)

        number_of_targets1 = len(targets1)
        number_of_targets2 = len(targets2)
        number_of_targets3 = len(targets3)
        number_of_targets4 = len(targets4)
        number_of_targets5 = len(targets5)

        # Then
        self.assertIn(node1_id, build_graph.nodes())
        self.assertIn(node2_id, build_graph.nodes())
        self.assertIn(node3_id, build_graph.nodes())
        self.assertIn(node4_id, build_graph.nodes())
        self.assertIn(node5_id, build_graph.nodes())

        self.assertEqual(expected_number_of_parents1, number_of_parents1)
        self.assertEqual(expected_number_of_parents2, number_of_parents2)
        self.assertEqual(expected_number_of_parents3, number_of_parents3)
        self.assertEqual(expected_number_of_parents4, number_of_parents4)
        self.assertEqual(expected_number_of_parents5, number_of_parents5)

        self.assertEqual(number_of_dependencies1,
                         expected_number_of_dependencies1)
        self.assertEqual(number_of_dependencies2,
                         expected_number_of_dependencies2)
        self.assertEqual(number_of_dependencies3,
                         expected_number_of_dependencies3)
        self.assertEqual(number_of_dependencies4,
                         expected_number_of_dependencies4)
        self.assertEqual(number_of_dependencies5,
                         expected_number_of_dependencies5)

        self.assertEqual(expected_number_of_targets1, number_of_targets1)
        self.assertEqual(expected_number_of_targets2, number_of_targets2)
        self.assertEqual(expected_number_of_targets3, number_of_targets3)
        self.assertEqual(expected_number_of_targets4, number_of_targets4)
        self.assertEqual(expected_number_of_targets5, number_of_targets5)


    @unittest.skip("Need to fix a logic problem here, skipping for now")
    def test_diamond_redundancy(self):
        # Note: This test fails because multiple Expanders are being constructed during the graph expansion instead
        # of using the expanders that are already present inside the rule dependency graph. This is the output from
        # printing out each Expander's unexpanded_id and id(self) inside the expander's expand() method:
        # diamond_redundancy_bottom_target 69197328
        # diamond_redundancy_middle_target_01 69197328
        # diamond_redundancy_middle_target_02 69197392
        # diamond_redundancy_middle_target_01 69197328
        # diamond_redundancy_top_target 69197328
        # diamond_redundancy_top_target 69203472
        # diamond_redundancy_highest_target 69203472
        # diamond_redundancy_highest_target 69204432
        # diamond_redundancy_super_target 69204432
        # diamond_redundancy_middle_target_02 69197328
        # diamond_redundancy_top_target 69197328
        # Given
        jobs = [
            SimpleTimestampExpandedTestJob("diamond_redundant_bottom_job", file_step="5min",
                expander_type=builder.expanders.TimestampExpander,
                targets=[{"unexpanded_id": "diamond_redundancy_bottom_target", "file_step": "5min"}],
                depends=[{"unexpanded_id": "diamond_redundancy_middle_target_01", "file_step": "5min"},
                         {"unexpanded_id": "diamond_redundancy_middle_target_02", "file_step": "5min"}]),
            SimpleTimestampExpandedTestJob("diamond_redundant_middle_job_01", file_step="5min",
                expander_type=builder.expanders.TimestampExpander,
                targets=[{"unexpanded_id": "diamond_redundancy_middle_target_01", "file_step": "5min"}],
                depends=[{"unexpanded_id": "diamond_redundancy_top_target", "file_step": "5min"}]),
            SimpleTimestampExpandedTestJob("diamond_redundant_middle_job_02", file_step="5min",
                expander_type=builder.expanders.TimestampExpander,
                targets=[{"unexpanded_id": "diamond_redundancy_middle_target_02", "file_step": "5min"}],
                depends=[{"unexpanded_id": "diamond_redundancy_top_target", "file_step": "5min"}]),
            SimpleTimestampExpandedTestJob("diamond_redundant_top_job", file_step="5min",
                expander_type=builder.expanders.TimestampExpander,
                targets=[{"unexpanded_id": "diamond_redundancy_top_target", "file_step": "5min"}],
                depends=[{"unexpanded_id": "diamond_redundancy_highest_target", "file_step": "5min"}]),
            SimpleTimestampExpandedTestJob("diamond_redundant_highest_job", file_step="5min",
                expander_type=builder.expanders.TimestampExpander,
                targets=[{"unexpanded_id": "diamond_redundancy_highest_target", "file_step": "5min"}],
                depends=[{"unexpanded_id": "diamond_redundancy_super_target", "file_step": "5min"}])
        ]
        mock_jobs = []
        for job in jobs:
            job.expand = mock.MagicMock(wraps=job.expand)
            mock_jobs.append(job)
        build_context = {
           "start_time": arrow.get("2014-12-05T10:50"),
           "end_time": arrow.get("2014-12-05T10:55"),
        }

        build_manager = builder.build.BuildManager(mock_jobs, [])
        rdg = build_manager.rule_dependency_graph
        for k in rdg.node:
            if isinstance(rdg.node[k]['object'], builder.expanders.Expander):
                rdg.node[k]['object'].expand = mock.Mock(wraps=rdg.node[k]['object'].expand)
                print "Replacing expand on {}".format(k)
        print "Replaced expand method"
        build = build_manager.make_build()

        expected_call_count1 = 1
        expected_call_count2 = 1
        expected_call_count3 = 3
        expected_call_count4 = 2
        expected_call_count5 = 1

        # When
        build.add_job("diamond_redundant_bottom_job", build_context)
        call_count1 = mock_jobs[3].expand.call_count
        call_count2 = mock_jobs[4].expand.call_count
        call_count3 = (build.rule_dependency_graph.node
                ["diamond_redundancy_top_target"]
                ["object"]
                .expand.call_count)
        call_count4 = (build.rule_dependency_graph.node
                ["diamond_redundancy_highest_target"]
                ["object"]
                .expand.call_count)
        call_count5 = (build.rule_dependency_graph.node
                ["diamond_redundancy_super_target"]
                ["object"]
                .expand.call_count)

        # Then
        self.assertEqual(call_count1, expected_call_count1)
        self.assertEqual(call_count2, expected_call_count2)
        self.assertEqual(call_count3, expected_call_count3)
        self.assertEqual(call_count4, expected_call_count4)
        self.assertEqual(call_count5, expected_call_count5)

    def test_stale(self):
        # Given
        current_time = 600
        jobs1 = [
            SimpleTimestampExpandedTestJob("stale_standard_job", file_step="15min", cache_time="5min",
                expander_type=builder.expanders.TimestampExpander,
                targets=[{"unexpanded_id": "stale_standard_target-%Y-%m-%d-%H-%M", "file_step": "5min"}],
                depends=[{"unexpanded_id": "stale_top_target-%Y-%m-%d-%H-%M", "file_step": "5min"}]),
        ]

        jobs2 = [
            SimpleTimestampExpandedTestJob("stale_ignore_mtime_job", file_step="15min",
                expander_type=builder.expanders.TimestampExpander,
                depends=[{"unexpanded_id": "stale_ignore_mtime_input_target_01-%Y-%m-%d-%H-%M", "file_step": "5min"},
                    {"unexpanded_id": "stale_ignore_mtime_input_target_02-%Y-%m-%d-%H-%M", "file_step": "5min", "ignore_mtime": True}],
                targets=[{"unexpanded_id": "stale_ignore_mtime_output_target-%Y-%m-%d-%H-%M", "file_step": "5min"}]),
        ]

        build_context1 = {
            "start_time": arrow.get("2014-12-05T10:50"),
            "end_time": arrow.get("2014-12-05T10:50"),
        }

        build_context2 = {
            "start_time": arrow.get("2014-12-05T10:50"),
            "end_time": arrow.get("2014-12-05T10:50"),
        }

        build_manager1 = builder.build.BuildManager(jobs1, [])
        build_manager2 = builder.build.BuildManager(jobs2, [])
        build1 = build_manager1.make_build()
        build2 = build_manager1.make_build()
        build3 = build_manager1.make_build()
        build4 = build_manager1.make_build()
        build5 = build_manager1.make_build()
        build6 = build_manager2.make_build()
        build7 = build_manager2.make_build()

        build1.add_job("stale_standard_job", build_context1)
        build2.add_job("stale_standard_job", build_context1)
        build3.add_job("stale_standard_job", build_context1)
        build4.add_job("stale_standard_job", build_context1)
        build5.add_job("stale_standard_job", build_context1)
        build6.add_job("stale_ignore_mtime_job", build_context2)
        build7.add_job("stale_ignore_mtime_job", build_context2)

        # all deps, all targets, all deps are older than targets
        expected_stale1 = False
        (build1.node
                ["stale_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build1.node
                ["stale_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build1.node
                ["stale_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_top_target-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build1.node
                ["stale_standard_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_standard_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_standard_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_standard_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_standard_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_standard_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        # not buildable, still not stale
        expected_stale2 = False
        (build2.node
                ["stale_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = False
        (build2.node
                ["stale_top_target-2014-12-05-10-45"]
                ["object"].mtime) = None
        (build2.node
                ["stale_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build2.node
                ["stale_top_target-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build2.node
                ["stale_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = False
        (build2.node
                ["stale_top_target-2014-12-05-10-55"]
                ["object"].mtime) = None
        (build2.node
                ["stale_standard_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_standard_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_standard_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_standard_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_standard_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_standard_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        # all deps, all targets, one dep newer than one target
        expected_stale3 = True
        (build3.node
                ["stale_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build3.node
                ["stale_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build3.node
                ["stale_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_top_target-2014-12-05-10-55"]
                ["object"].mtime) = 120
        (build3.node
                ["stale_standard_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_standard_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_standard_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_standard_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_standard_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_standard_target-2014-12-05-10-55"]
                ["object"].mtime) = 110
        # all deps, one missing target, all targets newer than deps
        expected_stale4 = True
        (build4.node
                ["stale_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build4.node
                ["stale_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build4.node
                ["stale_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_top_target-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build4.node
                ["stale_standard_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_standard_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_standard_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_standard_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_standard_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = False
        (build4.node
                ["stale_standard_target-2014-12-05-10-55"]
                ["object"].mtime) = None
        # all targets are within the cache_time
        expected_stale5 = False
        (build5.node
                ["stale_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build5.node
                ["stale_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 600
        (build5.node
                ["stale_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build5.node
                ["stale_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 600
        (build5.node
                ["stale_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build5.node
                ["stale_top_target-2014-12-05-10-55"]
                ["object"].mtime) = 600
        (build5.node
                ["stale_standard_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build5.node
                ["stale_standard_target-2014-12-05-10-45"]
                ["object"].mtime) = 500
        (build5.node
                ["stale_standard_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build5.node
                ["stale_standard_target-2014-12-05-10-50"]
                ["object"].mtime) = 500
        (build5.node
                ["stale_standard_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build5.node
                ["stale_standard_target-2014-12-05-10-55"]
                ["object"].mtime) = 500
        # target is older than an ignored mtime
        expected_stale6 = False
        (build6.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build6.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build6.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build6.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build6.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build6.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build6.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build6.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build6.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build6.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-50"]
                ["object"].mtime) = 120
        (build6.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build6.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build6.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build6.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build6.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build6.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build6.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build6.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-55"]
                ["object"].mtime) = 110
        # target is older than an non ignored mtime
        expected_stale7 = True
        (build7.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build7.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build7.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build7.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-50"]
                ["object"].mtime) = 120
        (build7.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build7.node
                ["stale_ignore_mtime_input_target_01-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build7.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build7.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build7.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build7.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-50"]
                ["object"].mtime) = 120
        (build7.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build7.node
                ["stale_ignore_mtime_input_target_02-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build7.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build7.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build7.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build7.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build7.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build7.node
                ["stale_ignore_mtime_output_target-2014-12-05-10-55"]
                ["object"].mtime) = 110


        old_arrow_get = copy.deepcopy(arrow.get)
        def new_arrow_get(*args, **kwargs):
            """This wraps the original arrow get so we can override only
            arrow get with no args
            """
            if not args and not kwargs:
                return arrow.get(current_time)
            else:
                return old_arrow_get(*args, **kwargs)


        # When
        with mock.patch("arrow.get", new_arrow_get):
            stale1 = (build1.node
                    ["stale_standard_job_2014-12-05-10-45-00"]
                    ["object"].get_stale())
            stale2 = (build2.node
                    ["stale_standard_job_2014-12-05-10-45-00"]
                    ["object"].get_stale())
            stale3 = (build3.node
                    ["stale_standard_job_2014-12-05-10-45-00"]
                    ["object"].get_stale())
            stale4 = (build4.node
                    ["stale_standard_job_2014-12-05-10-45-00"]
                    ["object"].get_stale())
            stale5 = (build5.node
                    ["stale_standard_job_2014-12-05-10-45-00"]
                    ["object"].get_stale())
            stale6 = (build6.node
                    ["stale_ignore_mtime_job_2014-12-05-10-45-00"]
                    ["object"].get_stale())
            stale7 = (build7.node
                    ["stale_ignore_mtime_job_2014-12-05-10-45-00"]
                    ["object"].get_stale())

        # Then
        self.assertEqual(stale1, expected_stale1)
        self.assertEqual(stale2, expected_stale2)
        self.assertEqual(stale3, expected_stale3)
        self.assertEqual(stale4, expected_stale4)
        self.assertEqual(stale5, expected_stale5)
        self.assertEqual(stale6, expected_stale6)
        self.assertEqual(stale7, expected_stale7)


    def _get_stale_alternate_jobs(self):
        return [
            SimpleTimestampExpandedTestJob("stale_alternate_bottom_job", file_step="15min",
               expander_type=builder.expanders.TimestampExpander, target_type=builder.targets.LocalFileSystemTarget,
               depends=[{'unexpanded_id': 'stale_alternate_top_target-%Y-%m-%d-%H-%M', 'file_step': '5min'},
                        {'unexpanded_id': 'stale_alternate_secondary_target-%Y-%m-%d-%H-%M', 'file_step': '5min'}],
               targets=[{'unexpanded_id': 'stale_alternate_bottom_target-%Y-%m-%d-%H-%M', 'file_step': '5min'}]),
            SimpleTimestampExpandedTestJob("stale_alternate_top_job", file_step="15min",
               expander_type=builder.expanders.TimestampExpander, target_type=builder.targets.LocalFileSystemTarget,
               depends=[{'unexpanded_id': 'stale_alternate_highest_target-%Y-%m-%d-%H-%M', 'file_step': '5min'}],
               targets=[{'unexpanded_id': 'stale_alternate_top_target-%Y-%m-%d-%H-%M', 'file_step': '5min'},
                    {'unexpanded_id': 'stale_alternate_bottom_target-%Y-%m-%d-%H-%M',
                     'file_step': '5min', 'type': 'alternates'}])
        ]
    def test_stale_alternate(self):
        # Given
        jobs1 = self._get_stale_alternate_jobs()

        build_context1 = {
            "start_time": arrow.get("2014-12-05T10:50"),
            "end_time": arrow.get("2014-12-05T10:50"),
            "start_job": "stale_alternate_bottom_job", # StaleAlternateBottomJobTester,
        }

        build_manager = builder.build.BuildManager(jobs1, [])
        build1 = build_manager.make_build()
        build2 = build_manager.make_build()
        build3 = build_manager.make_build()
        build4 = build_manager.make_build()

        build1.add_job("stale_alternate_bottom_job", build_context1)
        build2.add_job("stale_alternate_bottom_job", build_context1)
        build3.add_job("stale_alternate_bottom_job", build_context1)
        build4.add_job("stale_alternate_bottom_job", build_context1)

        # All alternates exist and are stale but the targets are not
        expected_stale1 = False
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].mtime) = 50
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].mtime) = 50
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].mtime) = 50

        # All alternates exist and are stale but the targets are not
        # and a single target does not exist
        expected_stale2 = True
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = False
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].mtime) = None
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].mtime) = 50
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].mtime) = 50
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].mtime) = 50

        # All alternates exist and are stale but the targets are not
        # and a single alternate does not exist
        expected_stale3 = False
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].mtime) = 50
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].mtime) = 50

        # All alternates exist and are stale but the targets are not
        # and a single alternate does not exist and a single target
        # (not corresponding) does not exist
        expected_stale4 = True
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = False
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].mtime) = None
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].mtime) = 50
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].mtime) = 50

        # When
        stale1 = (build1.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        stale2 = (build2.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        stale3 = (build3.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        stale4 = (build4.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())

        # Then
        self.assertEqual(stale1, expected_stale1)
        self.assertEqual(stale2, expected_stale2)
        self.assertEqual(stale3, expected_stale3)
        self.assertEqual(stale4, expected_stale4)

    def test_stale_alternate_update(self):
        # Given
        jobs1 = self._get_stale_alternate_jobs()

        build_context1 = {
            "start_time": arrow.get("2014-12-05T10:50"),
            "end_time": arrow.get("2014-12-05T10:50"),
        }

        build_manager = builder.build.BuildManager(jobs1, [])
        build1 = build_manager.make_build()
        build2 = build_manager.make_build()
        build3 = build_manager.make_build()
        build4 = build_manager.make_build()

        build1.add_job("stale_alternate_bottom_job", build_context1)
        build2.add_job("stale_alternate_bottom_job", build_context1)
        build3.add_job("stale_alternate_bottom_job", build_context1)
        build4.add_job("stale_alternate_bottom_job", build_context1)

        # All alternate_updates exist and are stale but the targets are not
        # All targets exist
        expected_original_stale1 = False
        expected_stale1 = False
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].mtime) = 50
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].mtime) = 50
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].mtime) = 50

        # All alternate_updates exist and are stale but the targets are not
        # and a single target does not exist
        expected_original_stale2 = True
        expected_stale2 = True
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = False
        (build2.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].mtime) = None
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].mtime) = 50
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].mtime) = 50
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].mtime) = 50

        # All alternate_updates exist and are stale but the targets are not
        # and a single target not form the job doesn't exist
        expected_original_stale3 = False
        expected_stale3 = False
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].mtime) = 50
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].mtime) = 50
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].mtime) = 50

        # The job is not stale, missing a target, all alternate_updates exist
        # alternate_update is not stale
        expected_original_stale4 = False
        expected_stale4 = False
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_highest_target-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = False
        (build4.node
                ["stale_alternate_top_target-2014-12-05-10-55"]
                ["object"].mtime) = None
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-45"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-50"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_secondary_target-2014-12-05-10-55"]
                ["object"].mtime) = 150
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-45"]
                ["object"].mtime) = 200
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-50"]
                ["object"].mtime) = 200
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build4.node
                ["stale_alternate_bottom_target-2014-12-05-10-55"]
                ["object"].mtime) = 200

        # When
        original_stale1 = (build1.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        (build1.node
                ["stale_alternate_bottom_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        stale1 = (build1.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        original_stale2 = (build2.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        (build2.node
                ["stale_alternate_bottom_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        stale2 = (build2.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        original_stale3 = (build3.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        (build3.node
                ["stale_alternate_bottom_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        stale3 = (build3.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        original_stale4 = (build4.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        (build4.node
                ["stale_alternate_bottom_job_2014-12-05-10-45-00"]
                ["object"].get_stale())
        stale4 = (build4.node
                ["stale_alternate_top_job_2014-12-05-10-45-00"]
                ["object"].get_stale())

        # Then
        self.assertEqual(original_stale1, expected_original_stale1)
        self.assertEqual(original_stale2, expected_original_stale2)
        self.assertEqual(original_stale3, expected_original_stale3)
        self.assertEqual(original_stale4, expected_original_stale4)
        self.assertEqual(stale1, expected_stale1)
        self.assertEqual(stale2, expected_stale2)
        self.assertEqual(stale3, expected_stale3)
        self.assertEqual(stale4, expected_stale4)

    def test_buildable(self):
        # Given
        jobs1 = [
            SimpleTimestampExpandedTestJob("buildable_job", file_step="15min",
                depends=[{"unexpanded_id": "buildable_15_minute_target_01-%Y-%m-%d-%H-%M", "file_step": "15min"},
                    {"unexpanded_id": "buildable_5_minute_target_01-%Y-%m-%d-%H-%M", "file_step": "5min"},
                    {"unexpanded_id": "buildable_15_minute_target_02-%Y-%m-%d-%H-%M", "file_step": "15min",
                        "type": "depends_one_or_more"},
                    {"unexpanded_id": "buildable_5_minute_target_02-%Y-%m-%d-%H-%M", "file_step": "5min",
                        "type": "depends_one_or_more"}])
        ]

        build_context1 = {
            "start_time": arrow.get("2014-12-05T10:50"),
            "end_time": arrow.get("2014-12-05T10:50"),
            "start_job": "buildable_job", # BuildableJobTester,
        }

        build_manager = builder.build.BuildManager(jobs1, [])

        build1 = build_manager.make_build()
        build2 = build_manager.make_build()
        build3 = build_manager.make_build()
        build4 = build_manager.make_build()
        build5 = build_manager.make_build()
        build6 = build_manager.make_build()

        build1.add_job("buildable_job", build_context1)
        build2.add_job("buildable_job", build_context1)
        build3.add_job("buildable_job", build_context1)
        build4.add_job("buildable_job", build_context1)
        build5.add_job("buildable_job", build_context1)
        build6.add_job("buildable_job", build_context1)

        # depends 15 minute not met
        expected_buildable1 = False
        (build1.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = False
        (build1.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = None
        (build1.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build1.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build1.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build1.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build1.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build1.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build1.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].mtime) = 100

        # depends 5 minute not met
        expected_buildable2 = False
        (build2.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build2.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build2.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build2.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build2.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build2.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build2.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build2.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build2.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].mtime) = 100

        # depends one or more 15 not met
        expected_buildable3 = False
        (build3.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build3.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build3.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build3.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build3.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = False
        (build3.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = None
        (build3.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build3.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build3.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].mtime) = 100

        # depends one or more 5 not met
        expected_buildable4 = False
        (build4.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = False
        (build4.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build4.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build4.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build4.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build4.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build4.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build4.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build4.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = False
        (build4.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = None
        (build4.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build4.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build4.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = False
        (build4.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].mtime) = None

        # all not met
        expected_buildable5 = False
        (build5.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = False
        (build5.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = None
        (build5.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build5.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build5.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build5.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build5.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build5.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build5.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = False
        (build5.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = None
        (build5.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = False
        (build5.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = None
        (build5.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build5.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build5.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = False
        (build5.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].mtime) = None

        # all met
        expected_buildable6 = True
        (build6.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = 100
        (build6.node
                ["buildable_15_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = True
        (build6.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build6.node
                ["buildable_5_minute_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build6.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build6.node
                ["buildable_5_minute_target_01-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build6.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build6.node
                ["buildable_5_minute_target_01-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build6.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build6.node
                ["buildable_15_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build6.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = False
        (build6.node
                ["buildable_5_minute_target_02-2014-12-05-10-45"]
                ["object"].mtime) = None
        (build6.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build6.node
                ["buildable_5_minute_target_02-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build6.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build6.node
                ["buildable_5_minute_target_02-2014-12-05-10-55"]
                ["object"].mtime) = 100

        # When
        buildable1 = (build1.node
                ["buildable_job_2014-12-05-10-45-00"]
                ["object"].get_buildable())
        buildable2 = (build2.node
                ["buildable_job_2014-12-05-10-45-00"]
                ["object"].get_buildable())
        buildable3 = (build3.node
                ["buildable_job_2014-12-05-10-45-00"]
                ["object"].get_buildable())
        buildable4 = (build4.node
                ["buildable_job_2014-12-05-10-45-00"]
                ["object"].get_buildable())
        buildable5 = (build5.node
                ["buildable_job_2014-12-05-10-45-00"]
                ["object"].get_buildable())
        buildable6 = (build6.node
                ["buildable_job_2014-12-05-10-45-00"]
                ["object"].get_buildable())

        # Then
        self.assertEqual(buildable1, expected_buildable1)
        self.assertEqual(buildable2, expected_buildable2)
        self.assertEqual(buildable3, expected_buildable3)
        self.assertEqual(buildable4, expected_buildable4)
        self.assertEqual(buildable5, expected_buildable5)
        self.assertEqual(buildable6, expected_buildable6)

    def test_past_cache_time(self):
        # Given
        current_time = 400

        build_context1 = {
            "start_time": arrow.get("2014-12-05T10:55"),
            "end_time": arrow.get("2014-12-05T10:55"),
        }
        jobs1 = [
            SimpleTimestampExpandedTestJob('past_cache_time_job', file_step="15min", cache_time="5min",
                expander_type=builder.expanders.TimestampExpander,
                targets=[{'unexpanded_id': 'past_cache_time_target-%Y-%m-%d-%H-%M', 'file_step': '5min'}]),
        ]

        build_manager = builder.build.BuildManager(jobs1, [])
        build1 = build_manager.make_build()
        build2 = build_manager.make_build()
        build3 = build_manager.make_build()

        build1.add_job("past_cache_time_job", build_context1)
        build2.add_job("past_cache_time_job", build_context1)
        build3.add_job("past_cache_time_job", build_context1)

        # a target doesn't exist
        expected_past_cache_time1 = True
        (build1.node
                ["past_cache_time_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["past_cache_time_target-2014-12-05-10-45"]
                ["object"].mtime) = 400
        (build1.node
                ["past_cache_time_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["past_cache_time_target-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build1.node
                ["past_cache_time_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["past_cache_time_target-2014-12-05-10-55"]
                ["object"].mtime) = 400

        # all targets are within the allowed time
        expected_past_cache_time2 = False
        (build2.node
                ["past_cache_time_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["past_cache_time_target-2014-12-05-10-45"]
                ["object"].mtime) = 400
        (build2.node
                ["past_cache_time_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["past_cache_time_target-2014-12-05-10-50"]
                ["object"].mtime) = 400
        (build2.node
                ["past_cache_time_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["past_cache_time_target-2014-12-05-10-55"]
                ["object"].mtime) = 400

        # no target is within the allowed time
        expected_past_cache_time3 = True
        (build3.node
                ["past_cache_time_target-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["past_cache_time_target-2014-12-05-10-45"]
                ["object"].mtime) = 50
        (build3.node
                ["past_cache_time_target-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["past_cache_time_target-2014-12-05-10-50"]
                ["object"].mtime) = 50
        (build3.node
                ["past_cache_time_target-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["past_cache_time_target-2014-12-05-10-55"]
                ["object"].mtime) = 50

        old_arrow_get = copy.deepcopy(arrow.get)
        def new_arrow_get(*args, **kwargs):
            """This wraps the original arrow get so we can override only
            arrow get with no args
            """
            if not args and not kwargs:
                return arrow.get(current_time)
            else:
                return old_arrow_get(*args, **kwargs)

        mock_arrow_get = mock.Mock(side_effect=new_arrow_get)

        # When
        with mock.patch("arrow.get", mock_arrow_get):
            past_cache_time1 = (build1.node
                    ["past_cache_time_job_2014-12-05-10-45-00"]
                    ["object"].past_cache_time())
            past_cache_time2 = (build2.node
                    ["past_cache_time_job_2014-12-05-10-45-00"]
                    ["object"].past_cache_time())
            past_cache_time3 = (build3.node
                    ["past_cache_time_job_2014-12-05-10-45-00"]
                    ["object"].past_cache_time())

        # Then
        self.assertEqual(past_cache_time1, expected_past_cache_time1)
        self.assertEqual(past_cache_time2, expected_past_cache_time2)
        self.assertEqual(past_cache_time3, expected_past_cache_time3)

    def test_all_dependencies(self):
        # Given
        jobs1 = [
            SimpleTimestampExpandedTestJob("all_dependencies_job", file_step="15min",
            expander_type=builder.expanders.TimestampExpander,
            depends=[
                {'unexpanded_id': 'all_dependencies_target_02-%Y-%m-%d-%H-%M', 'file_step': '5min', 'type': 'depends_one_or_more'},
                {'unexpanded_id': 'all_dependencies_target_01-%Y-%m-%d-%H-%M', 'file_step': '5min'}]),
        ]

        build_context1 = {
            "start_time": "2014-12-05T10:55",
            "end_time": "2014-12-05T10:55",
        }

        build_manager = builder.build.BuildManager(jobs1, [])

        build1 = build_manager.make_build()
        build2 = build_manager.make_build()
        build3 = build_manager.make_build()
        build4 = build_manager.make_build()

        build1.add_job("all_dependencies_job", build_context1)
        build2.add_job("all_dependencies_job", build_context1)
        build3.add_job("all_dependencies_job", build_context1)
        build4.add_job("all_dependencies_job", build_context1)

        # all dependencies
        expected_all_dependencies1 = True
        (build1.node
                ["all_dependencies_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["all_dependencies_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build1.node
                ["all_dependencies_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["all_dependencies_target_01-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build1.node
                ["all_dependencies_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["all_dependencies_target_01-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build1.node
                ["all_dependencies_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build1.node
                ["all_dependencies_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build1.node
                ["all_dependencies_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build1.node
                ["all_dependencies_target_02-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build1.node
                ["all_dependencies_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build1.node
                ["all_dependencies_target_02-2014-12-05-10-55"]
                ["object"].mtime) = 100
        # 01 missing one target
        expected_all_dependencies2 = False
        (build2.node
                ["all_dependencies_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["all_dependencies_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build2.node
                ["all_dependencies_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build2.node
                ["all_dependencies_target_01-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build2.node
                ["all_dependencies_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["all_dependencies_target_01-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build2.node
                ["all_dependencies_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build2.node
                ["all_dependencies_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build2.node
                ["all_dependencies_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build2.node
                ["all_dependencies_target_02-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build2.node
                ["all_dependencies_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build2.node
                ["all_dependencies_target_02-2014-12-05-10-55"]
                ["object"].mtime) = 100
        # 02 missing one target
        expected_all_dependencies3 = True
        (build3.node
                ["all_dependencies_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["all_dependencies_target_01-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build3.node
                ["all_dependencies_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["all_dependencies_target_01-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build3.node
                ["all_dependencies_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["all_dependencies_target_01-2014-12-05-10-55"]
                ["object"].mtime) = 100
        (build3.node
                ["all_dependencies_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = True
        (build3.node
                ["all_dependencies_target_02-2014-12-05-10-45"]
                ["object"].mtime) = 100
        (build3.node
                ["all_dependencies_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = True
        (build3.node
                ["all_dependencies_target_02-2014-12-05-10-50"]
                ["object"].mtime) = 100
        (build3.node
                ["all_dependencies_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = True
        (build3.node
                ["all_dependencies_target_02-2014-12-05-10-55"]
                ["object"].mtime) = 100
        # all deps missing
        expected_all_dependencies4 = False
        (build4.node
                ["all_dependencies_target_01-2014-12-05-10-45"]
                ["object"].cached_mtime) = False
        (build4.node
                ["all_dependencies_target_01-2014-12-05-10-45"]
                ["object"].mtime) = None
        (build4.node
                ["all_dependencies_target_01-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build4.node
                ["all_dependencies_target_01-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build4.node
                ["all_dependencies_target_01-2014-12-05-10-55"]
                ["object"].cached_mtime) = False
        (build4.node
                ["all_dependencies_target_01-2014-12-05-10-55"]
                ["object"].mtime) = None
        (build4.node
                ["all_dependencies_target_02-2014-12-05-10-45"]
                ["object"].cached_mtime) = False
        (build4.node
                ["all_dependencies_target_02-2014-12-05-10-45"]
                ["object"].mtime) = None
        (build4.node
                ["all_dependencies_target_02-2014-12-05-10-50"]
                ["object"].cached_mtime) = False
        (build4.node
                ["all_dependencies_target_02-2014-12-05-10-50"]
                ["object"].mtime) = None
        (build4.node
                ["all_dependencies_target_02-2014-12-05-10-55"]
                ["object"].cached_mtime) = False
        (build4.node
                ["all_dependencies_target_02-2014-12-05-10-55"]
                ["object"].mtime) = None

        # When
        all_dependencies1 = (build1.node
                ["all_dependencies_job_2014-12-05-10-45-00"]
                ["object"].all_dependencies())
        all_dependencies2 = (build2.node
                ["all_dependencies_job_2014-12-05-10-45-00"]
                ["object"].all_dependencies())
        all_dependencies3 = (build3.node
                ["all_dependencies_job_2014-12-05-10-45-00"]
                ["object"].all_dependencies())
        all_dependencies4 = (build4.node
                ["all_dependencies_job_2014-12-05-10-45-00"]
                ["object"].all_dependencies())

        # Then
        self.assertEqual(all_dependencies1, expected_all_dependencies1)
        self.assertEqual(all_dependencies2, expected_all_dependencies2)
        self.assertEqual(all_dependencies3, expected_all_dependencies3)
        self.assertEqual(all_dependencies4, expected_all_dependencies4)

    def test_should_run_logic(self):
        # Given
        build_context = {}
        ShouldRunLogicJobTester = lambda: SimpleTestJobDefinition('should_run_logic')
        ShouldRunCacheLogicJobTester = lambda: SimpleTestJobDefinition('should_run_cache_logic', cache_time='5min')

        graph1 = networkx.DiGraph()
        graph2 = networkx.DiGraph()
        graph3 = networkx.DiGraph()
        graph4 = networkx.DiGraph()
        graph5 = networkx.DiGraph()
        graph6 = networkx.DiGraph()
        graph7 = networkx.DiGraph()
        graph8 = networkx.DiGraph()
        graph9 = networkx.DiGraph()
        graph10 = networkx.DiGraph()
        graph11 = networkx.DiGraph()
        graph12 = networkx.DiGraph()
        graph13 = networkx.DiGraph()
        graph14 = networkx.DiGraph()

        job1 = ShouldRunLogicJobTester().expand(graph1, build_context)[0]
        job2 = ShouldRunLogicJobTester().expand(graph2, build_context)[0]
        job3 = ShouldRunLogicJobTester().expand(graph3, build_context)[0]
        job4 = ShouldRunLogicJobTester().expand(graph4, build_context)[0]
        job5 = ShouldRunLogicJobTester().expand(graph5, build_context)[0]
        job6 = ShouldRunLogicJobTester().expand(graph6, build_context)[0]
        job7 = ShouldRunCacheLogicJobTester().expand(graph7, build_context)[0]
        job8 = ShouldRunCacheLogicJobTester().expand(graph8, build_context)[0]
        job9 = ShouldRunCacheLogicJobTester().expand(graph9, build_context)[0]
        job10 = ShouldRunCacheLogicJobTester().expand(graph10, build_context)[0]
        job11 = ShouldRunCacheLogicJobTester().expand(graph11, build_context)[0]
        job12 = ShouldRunCacheLogicJobTester().expand(graph12, build_context)[0]
        job13 = ShouldRunLogicJobTester().expand(graph13, build_context)[0]
        job14 = ShouldRunCacheLogicJobTester().expand(graph14, build_context)[0]

        graph1.add_node("unique_id1", object=job1)
        graph2.add_node("unique_id2", object=job2)
        graph3.add_node("unique_id3", object=job3)
        graph4.add_node("unique_id4", object=job4)
        graph5.add_node("unique_id5", object=job5)
        graph6.add_node("unique_id6", object=job6)
        graph7.add_node("unique_id7", object=job7)
        graph8.add_node("unique_id8", object=job8)
        graph9.add_node("unique_id9", object=job9)
        graph10.add_node("unique_id10", object=job10)
        graph11.add_node("unique_id11", object=job11)
        graph12.add_node("unique_id12", object=job12)
        graph13.add_node("unique_id13", object=job13)
        graph14.add_node("unique_id14", object=job14)

        # all true
        expected_should_run1 = True
        graph1.node["unique_id1"]["object"].stale = True
        graph1.node["unique_id1"]["object"].buildable = True
        graph1.node["unique_id1"]["object"].past_curfew = mock.Mock(return_value=True)
        graph1.node["unique_id1"]["object"].all_dependencies = mock.Mock(return_value=True)
        graph1.node["unique_id1"]["object"].get_parents_should_run = mock.Mock(return_value=False)
        # not stale everything else true
        expected_should_run2 = False
        graph2.node["unique_id2"]["object"].stale = False
        graph2.node["unique_id2"]["object"].buildable = True
        graph2.node["unique_id2"]["object"].past_curfew = mock.Mock(return_value=True)
        graph2.node["unique_id2"]["object"].all_dependencies = mock.Mock(return_value=True)
        graph2.node["unique_id2"]["object"].get_parents_should_run = mock.Mock(return_value=False)
        # not buildable everything else true
        expected_should_run3 = False
        graph3.node["unique_id3"]["object"].stale = True
        graph3.node["unique_id3"]["object"].buildable = False
        graph3.node["unique_id3"]["object"].past_curfew = mock.Mock(return_value=True)
        graph3.node["unique_id3"]["object"].all_dependencies = mock.Mock(return_value=True)
        graph3.node["unique_id3"]["object"].get_parents_should_run = mock.Mock(return_value=False)
        # not past curfew everything else true
        expected_should_run4 = True
        graph4.node["unique_id4"]["object"].stale = True
        graph4.node["unique_id4"]["object"].buildable = True
        graph4.node["unique_id4"]["object"].past_curfew = mock.Mock(return_value=False)
        graph4.node["unique_id4"]["object"].all_dependencies = mock.Mock(return_value=True)
        graph4.node["unique_id4"]["object"].get_parents_should_run = mock.Mock(return_value=False)
        # not all dependencies everything else true
        expected_should_run5 = True
        graph5.node["unique_id5"]["object"].stale = True
        graph5.node["unique_id5"]["object"].buildable = True
        graph5.node["unique_id5"]["object"].past_curfew = mock.Mock(return_value=True)
        graph5.node["unique_id5"]["object"].all_dependencies = mock.Mock(return_value=False)
        graph5.node["unique_id5"]["object"].get_parents_should_run = mock.Mock(return_value=False)
        # parents should run, everything else is true
        expected_should_run6 = False
        graph6.node["unique_id6"]["object"].stale = True
        graph6.node["unique_id6"]["object"].buildable = True
        graph6.node["unique_id6"]["object"].past_curfew = mock.Mock(return_value=True)
        graph6.node["unique_id6"]["object"].all_dependencies = mock.Mock(return_value=True)
        graph6.node["unique_id6"]["object"].get_parents_should_run = mock.Mock(return_value=True)
        # cache not past curfew
        expected_should_run7 = True
        graph7.node["unique_id7"]["object"].stale = True
        graph7.node["unique_id7"]["object"].buildable = True
        graph7.node["unique_id7"]["object"].past_curfew = mock.Mock(return_value=False)
        graph7.node["unique_id7"]["object"].all_dependencies = mock.Mock(return_value=True)
        graph7.node["unique_id7"]["object"].past_cache_time = mock.Mock(return_value=True)
        graph7.node["unique_id7"]["object"].get_parents_should_run = mock.Mock(return_value=False)
        # cache not all dependencies
        expected_should_run8 = True
        graph8.node["unique_id8"]["object"].stale = True
        graph8.node["unique_id8"]["object"].buildable = True
        graph8.node["unique_id8"]["object"].past_curfew = mock.Mock(return_value=True)
        graph8.node["unique_id8"]["object"].all_dependencies = mock.Mock(return_value=False)
        graph8.node["unique_id8"]["object"].past_cache_time = mock.Mock(return_value=True)
        graph8.node["unique_id8"]["object"].get_parents_should_run = mock.Mock(return_value=False)
        # cache not stale
        expected_should_run9 = False
        graph9.node["unique_id9"]["object"].stale = False
        graph9.node["unique_id9"]["object"].buildable = True
        graph9.node["unique_id9"]["object"].past_curfew = mock.Mock(return_value=True)
        graph9.node["unique_id9"]["object"].all_dependencies = mock.Mock(return_value=True)
        graph9.node["unique_id9"]["object"].past_cache_time = mock.Mock(return_value=True)
        graph9.node["unique_id9"]["object"].get_parents_should_run = mock.Mock(return_value=False)
        # cache not buildable
        expected_should_run10 = False
        graph10.node["unique_id10"]["object"].stale = True
        graph10.node["unique_id10"]["object"].buildable = False
        graph10.node["unique_id10"]["object"].past_curfew = mock.Mock(return_value=True)
        graph10.node["unique_id10"]["object"].all_dependencies = mock.Mock(return_value=True)
        graph10.node["unique_id10"]["object"].past_cache_time = mock.Mock(return_value=True)
        graph10.node["unique_id10"]["object"].get_parents_should_run = mock.Mock(return_value=False)

        # all true not past cache
        expected_should_run11 = False
        graph11.node["unique_id11"]["object"].stale = False
        graph11.node["unique_id11"]["object"].buildable = True
        graph11.node["unique_id11"]["object"].past_curfew = mock.Mock(return_value=True)
        graph11.node["unique_id11"]["object"].all_dependencies = mock.Mock(return_value=True)
        graph11.node["unique_id11"]["object"].past_cache_time = mock.Mock(return_value=False)
        graph11.node["unique_id11"]["object"].get_parents_should_run = mock.Mock(return_value=False)
        # parents should run
        expected_should_run12 = False
        graph12.node["unique_id12"]["object"].stale = True
        graph12.node["unique_id12"]["object"].buildable = True
        graph12.node["unique_id12"]["object"].past_curfew = mock.Mock(return_value=True)
        graph12.node["unique_id12"]["object"].all_dependencies = mock.Mock(return_value=True)
        graph12.node["unique_id12"]["object"].past_cache_time = mock.Mock(return_value=True)
        graph12.node["unique_id12"]["object"].get_parents_should_run = mock.Mock(return_value=True)
        # not past curfew and not all dependencies
        expected_should_run13 = False
        graph13.node["unique_id13"]["object"].stale = True
        graph13.node["unique_id13"]["object"].buildable = True
        graph13.node["unique_id13"]["object"].past_curfew = mock.Mock(return_value=False)
        graph13.node["unique_id13"]["object"].all_dependencies = mock.Mock(return_value=False)
        graph13.node["unique_id13"]["object"].get_parents_should_run = mock.Mock(return_value=False)
        # not past curfew and not all dependencies has cache_time
        expected_should_run14 = True
        graph14.node["unique_id14"]["object"].stale = True
        graph14.node["unique_id14"]["object"].buildable = True
        graph14.node["unique_id14"]["object"].past_curfew = mock.Mock(return_value=False)
        graph14.node["unique_id14"]["object"].all_dependencies = mock.Mock(return_value=False)
        graph14.node["unique_id14"]["object"].get_parents_should_run = mock.Mock(return_value=False)

        # When
        should_run1 = graph1.node["unique_id1"]["object"].get_should_run()
        should_run2 = graph2.node["unique_id2"]["object"].get_should_run()
        should_run3 = graph3.node["unique_id3"]["object"].get_should_run()
        should_run4 = graph4.node["unique_id4"]["object"].get_should_run()
        should_run5 = graph5.node["unique_id5"]["object"].get_should_run()
        should_run6 = graph6.node["unique_id6"]["object"].get_should_run()
        should_run7 = graph7.node["unique_id7"]["object"].get_should_run()
        should_run8 = graph8.node["unique_id8"]["object"].get_should_run()
        should_run9 = graph9.node["unique_id9"]["object"].get_should_run()
        should_run10 = graph10.node["unique_id10"]["object"].get_should_run()
        should_run11 = graph11.node["unique_id11"]["object"].get_should_run()
        should_run12 = graph12.node["unique_id12"]["object"].get_should_run()
        should_run13 = graph13.node["unique_id13"]["object"].get_should_run()
        should_run14 = graph14.node["unique_id14"]["object"].get_should_run()

        # Then
        self.assertEqual(should_run1, expected_should_run1)
        self.assertEqual(should_run2, expected_should_run2)
        self.assertEqual(should_run3, expected_should_run3)
        self.assertEqual(should_run4, expected_should_run4)
        self.assertEqual(should_run5, expected_should_run5)
        self.assertEqual(should_run6, expected_should_run6)
        self.assertEqual(should_run7, expected_should_run7)
        self.assertEqual(should_run8, expected_should_run8)
        self.assertEqual(should_run9, expected_should_run9)
        self.assertEqual(should_run10, expected_should_run10)
        self.assertEqual(should_run11, expected_should_run11)
        self.assertEqual(should_run12, expected_should_run12)
        self.assertEqual(should_run13, expected_should_run13)
        self.assertEqual(should_run14, expected_should_run14)

    def test_past_curfew(self):
        # Given
        PastCurfewTimestampJobTester = lambda: SimpleTimestampExpandedTestJob("past_curfew_timestamp_job")
        PastCurfewJobTester = lambda: SimpleTestJobDefinition("past_curfew_job")
        build_graph = mock.Mock()
        build_context = {}
        job1 = PastCurfewJobTester().expand(build_graph, build_context)[0]
        job2 = PastCurfewTimestampJobTester().expand(build_graph,
                {
                    "start_time": arrow.get(400),
                    "end_time": arrow.get(400),
                })[0]
        job3 = PastCurfewTimestampJobTester().expand(build_graph,
                {
                    "start_time": arrow.get(100),
                    "end_time": arrow.get(100),
                })[0]

        current_time = 1000

        expected_past_curfew1 = True
        expected_past_curfew2 = False
        expected_past_curfew3 = True

        old_arrow_get = copy.deepcopy(arrow.get)
        def new_arrow_get(*args, **kwargs):
            """This wraps the original arrow get so we can override only
            arrow get with no args
            """
            if not args and not kwargs:
                return arrow.get(current_time)
            else:
                return old_arrow_get(*args, **kwargs)

        # When
        with mock.patch("arrow.get", new_arrow_get):
            past_curfew1 = job1.past_curfew()
            past_curfew2 = job2.past_curfew()
            past_curfew3 = job3.past_curfew()

        # Then
        self.assertEqual(past_curfew1, expected_past_curfew1)
        self.assertEqual(past_curfew2, expected_past_curfew2)
        self.assertEqual(past_curfew3, expected_past_curfew3)

    def test_should_run_recurse(self):
        # Given
        expander_type = builder.expanders.TimestampExpander
        target_type = builder.targets.LocalFileSystemTarget
        common_args = {'expander_type': expander_type, 'target_type': target_type}
        jobs1 = [
            ShouldRunRecurseJobDefinition('should_run_recurse_job_01',
                depends=[{'unexpanded_id': 'should_run_recurse_target_00', 'file_step': '5min'}],
                targets=[{'unexpanded_id': 'should_run_recurse_target_01', 'file_step': '5min'}],
                **common_args),
            ShouldRunRecurseJobDefinition('should_run_recurse_job_02',
                depends=[{'unexpanded_id': 'should_run_recurse_target_01', 'file_step': '5min'}],
                targets=[{'unexpanded_id': 'should_run_recurse_target_02', 'file_step': '5min'}],
                **common_args),
            ShouldRunRecurseJobDefinition('should_run_recurse_job_03',
                depends=[{'unexpanded_id': 'should_run_recurse_target_02', 'file_step': '5min'}],
                targets=[{'unexpanded_id': 'should_run_recurse_target_03', 'file_step': '5min'}],
                **common_args),
            ShouldRunRecurseJobDefinition('should_run_recurse_job_04',
                depends=[{'unexpanded_id': 'should_run_recurse_target_03', 'file_step': '5min'}],
                targets=[{'unexpanded_id': 'should_run_recurse_target_04', 'file_step': '5min'}],
                **common_args),
            ShouldRunRecurseJobDefinition('should_run_recurse_job_05',
                depends=[{'unexpanded_id': 'should_run_recurse_target_04', 'file_step': '5min'}],
                targets=[{'unexpanded_id': 'should_run_recurse_target_05', 'file_step': '5min'}],
                **common_args),
            ShouldRunRecurseJobDefinition('should_run_recurse_job_06',
                depends=[{'unexpanded_id': 'should_run_recurse_target_05', 'file_step': '5min'}],
                targets=[{'unexpanded_id': 'should_run_recurse_target_06', 'file_step': '5min'}],
                **common_args),
            ShouldRunRecurseJobDefinition('should_run_recurse_job_07',
                depends=[{'unexpanded_id': 'should_run_recurse_target_06', 'file_step': '5min'}],
                targets=[{'unexpanded_id': 'should_run_recurse_target_07', 'file_step': '5min'}],
                **common_args),
            ShouldRunRecurseJobDefinition('should_run_recurse_job_08',
                depends=[{'unexpanded_id': 'should_run_recurse_target_07', 'file_step': '5min'}],
                targets=[{'unexpanded_id': 'should_run_recurse_target_08', 'file_step': '5min'}],
                **common_args),
            ShouldRunRecurseJobDefinition('should_run_recurse_job_09',
                depends=[{'unexpanded_id': 'should_run_recurse_target_08', 'file_step': '5min'}],
                targets=[{'unexpanded_id': 'should_run_recurse_target_09', 'file_step': '5min'}],
                **common_args),
            ShouldRunRecurseJobDefinition('should_run_recurse_job_10',
                depends=[{'unexpanded_id': 'should_run_recurse_target_09', 'file_step': '5min'}],
                targets=[{'unexpanded_id': 'should_run_recurse_target_10', 'file_step': '5min'}],
                **common_args)
        ]
        for job, should_run_immediate in zip(jobs1, [False, True, False, True, False, True, False, True, False, True]):
            job.should_run_immediate = should_run_immediate

        build_context1 = {
            "start_time": arrow.get("2014-12-05T10:55"),
            "end_time": arrow.get("2014-12-05T10:55"),
            "start_job": "should_run_recurse_job_10", # ShouldRunRecurseJob10Tester,
        }

        build_manager = builder.build.BuildManager(jobs1, [])
        build1 = build_manager.make_build()

        build1.add_job("should_run_recurse_job_10", build_context1)

        expected_parents_should_run1 = False
        expected_parents_should_run2 = False
        expected_parents_should_run3 = True
        expected_parents_should_run4 = True
        expected_parents_should_run5 = True
        expected_parents_should_run6 = True
        expected_parents_should_run7 = True
        expected_parents_should_run8 = True
        expected_parents_should_run9 = True
        expected_parents_should_run10 = True

        # When
        parents_should_run1 = (build1.node
                ["should_run_recurse_job_01"]
                ["object"].get_parents_should_run())
        parents_should_run2 = (build1.node
                ["should_run_recurse_job_02"]
                ["object"].get_parents_should_run())
        parents_should_run3 = (build1.node
                ["should_run_recurse_job_03"]
                ["object"].get_parents_should_run())
        parents_should_run4 = (build1.node
                ["should_run_recurse_job_04"]
                ["object"].get_parents_should_run())
        parents_should_run5 = (build1.node
                ["should_run_recurse_job_05"]
                ["object"].get_parents_should_run())
        parents_should_run6 = (build1.node
                ["should_run_recurse_job_06"]
                ["object"].get_parents_should_run())
        parents_should_run7 = (build1.node
                ["should_run_recurse_job_07"]
                ["object"].get_parents_should_run())
        parents_should_run8 = (build1.node
                ["should_run_recurse_job_08"]
                ["object"].get_parents_should_run())
        parents_should_run9 = (build1.node
                ["should_run_recurse_job_09"]
                ["object"].get_parents_should_run())
        parents_should_run10 = (build1.node
                ["should_run_recurse_job_10"]
                ["object"].get_parents_should_run())

        # Then
        self.assertEqual(parents_should_run1,
                         expected_parents_should_run1)
        self.assertEqual(parents_should_run2,
                         expected_parents_should_run2)
        self.assertEqual(parents_should_run3,
                         expected_parents_should_run3)
        self.assertEqual(parents_should_run4,
                         expected_parents_should_run4)
        self.assertEqual(parents_should_run5,
                         expected_parents_should_run5)
        self.assertEqual(parents_should_run6,
                         expected_parents_should_run6)
        self.assertEqual(parents_should_run7,
                         expected_parents_should_run7)
        self.assertEqual(parents_should_run8,
                         expected_parents_should_run8)
        self.assertEqual(parents_should_run9,
                         expected_parents_should_run9)
        self.assertEqual(parents_should_run10,
                         expected_parents_should_run10)

    def test_expand_exact(self):
        # Given
        jobs = [
            SimpleTestJobDefinition(unexpanded_id="test_expand_exact_top",
                          targets=["test_expand_exact_top_target"],
                          depends=["test_expand_exact_highest_target"]),
            SimpleTestJobDefinition(unexpanded_id="test_expand_exact_middle",
                          targets=["test_expand_exact_middle_target"],
                          depends=["test_expand_exact_top_target"]),
            SimpleTestJobDefinition(unexpanded_id="test_expand_exact_bottom",
                          targets=["test_expand_exact_bottom_target"],
                          depends=["test_expand_exact_middle_target"])
        ]

        build_context = {
        }

        build_manager = builder.build.BuildManager(jobs, [])

        build = build_manager.make_build()

        # When
        build.add_job("test_expand_exact_middle", build_context, depth=1)

        # Then
        self.assertEqual(len(build.node), 4)


    def test_force_build(self):
        # Given
        jobs = [
            SimpleTimestampExpandedTestJob('force_build_top',
                file_step="1min",
                expander_type=builder.expanders.TimestampExpander,
                targets=[
                    {"unexpanded_id": "force_build_top_target_%Y-%m-%d-%H-%M", "file_step": "1min"}
            ]),
            SimpleTimestampExpandedTestJob('force_build_middle',
                file_step="5min",
                expander_type=builder.expanders.TimestampExpander,
                depends=[
                    {"unexpanded_id": "force_build_top_target_%Y-%m-%d-%H-%M", "file_step": "1min"}
                ],
                targets=[
                    {"unexpanded_id": "force_build_middle_target_%Y-%m-%d-%H-%M", "file_step": "5min"}
            ]),
            SimpleTimestampExpandedTestJob('force_build_bottom',
                file_step="15min",
                expander_type=builder.expanders.TimestampExpander,
                depends=[
                    {"unexpanded_id": "force_build_top_target_%Y-%m-%d-%H-%M", "file_step": "1min"},
                    {"unexpanded_id": "force_build_middle_target_%Y-%m-%d-%H-%M", "file_step": "5min"},
                ],
                targets=[
                    {"unexpanded_id": "force_build_bottom_target_%Y-%m-%d-%H-%M", "file_step": "15min"}
                ])
        ]

        build_context = {
                "start_time": arrow.get("2014-12-05T11:45"),
                "end_time": arrow.get("2014-12-05T12:15"),
        }

        build_manager = builder.build.BuildManager(jobs, [])
        build = build_manager.make_build()

        # When
        build.add_job("force_build_bottom", build_context, force=True, depth=2)

        # Then
        count = 0
        for node_id, node in build.node.iteritems():
            if node.get("object") is None:
                continue
            if not isinstance(node["object"], builder.jobs.Job):
                continue
            if "force_build_bottom" in node_id:
                count = count + 1
                self.assertTrue(node["object"].force)
            else:
                self.assertFalse(node["object"].build_context.get("force", False))
        self.assertEqual(count, 2)


    def test_ignore_produce(self):
        # Given
        jobs = [
            SimpleTestJobDefinition("ignore_produce_job",
                targets=["ignore_produce_marker_target"],
                targets_dict={
                    'untracked': [builder.expanders.Expander(
                        builder.targets.Target,
                        "ignore_produce_ignore_target")]
                })
        ]

        build_manager = builder.build.BuildManager(jobs, [])
        build1 = build_manager.make_build()
        build2 = build_manager.make_build()
        build3 = build_manager.make_build()
        build4 = build_manager.make_build()

        build_context1 = "ignore_produce_job"
        build_context2 = "ignore_produce_job"
        build_context3 = "ignore_produce_job"
        build_context4 = "ignore_produce_job"


        build1.add_job(build_context1, {})
        build2.add_job(build_context2, {})
        build3.add_job(build_context3, {})
        build4.add_job(build_context4, {})

        expected_stale1 = True
        build1.node["ignore_produce_ignore_target"]["object"].cached_mtime = True
        build1.node["ignore_produce_ignore_target"]["object"].mtime = None
        build1.node["ignore_produce_marker_target"]["object"].cached_mtime = True
        build1.node["ignore_produce_marker_target"]["object"].mtime = None

        expected_stale2 = True
        build2.node["ignore_produce_ignore_target"]["object"].cached_mtime = True
        build2.node["ignore_produce_ignore_target"]["object"].mtime = 100
        build2.node["ignore_produce_marker_target"]["object"].cached_mtime = True
        build2.node["ignore_produce_marker_target"]["object"].mtime = None

        expected_stale3 = False
        build3.node["ignore_produce_ignore_target"]["object"].cached_mtime = True
        build3.node["ignore_produce_ignore_target"]["object"].mtime = 100
        build3.node["ignore_produce_marker_target"]["object"].cached_mtime = True
        build3.node["ignore_produce_marker_target"]["object"].mtime = 100

        expected_stale4 = False
        build4.node["ignore_produce_ignore_target"]["object"].cached_mtime = True
        build4.node["ignore_produce_ignore_target"]["object"].mtime = None
        build4.node["ignore_produce_marker_target"]["object"].cached_mtime = True
        build4.node["ignore_produce_marker_target"]["object"].mtime = 100

        # When
        actual_stale1 = build1.node["ignore_produce_job"]["object"].get_stale()
        actual_stale2 = build2.node["ignore_produce_job"]["object"].get_stale()
        actual_stale3 = build3.node["ignore_produce_job"]["object"].get_stale()
        actual_stale4 = build4.node["ignore_produce_job"]["object"].get_stale()

        # Then
        self.assertEqual(actual_stale1, expected_stale1)
        self.assertEqual(actual_stale2, expected_stale2)
        self.assertEqual(actual_stale3, expected_stale3)
        self.assertEqual(actual_stale4, expected_stale4)

    def test_stale_with_no_targets(self):
        # Given
        targets1 = {}

        targets2 = {
            "alternates": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "target"
                )
            ]
        }
        targets3 = {
            "alternates": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "target"
                )
            ]
        }

        job1 = builder.jobs.JobDefinition(unexpanded_id="job_with_no_targets",
                                targets=targets1)
        job2 = builder.jobs.JobDefinition(unexpanded_id="job_with_no_targets",
                                targets=targets2)
        job3 = builder.jobs.JobDefinition(unexpanded_id="job_with_no_targets",
                                targets=targets3)

        build_manager1 = builder.build.BuildManager([job1], [])
        build_manager2 = builder.build.BuildManager([job2], [])
        build_manager3 = builder.build.BuildManager([job3], [])
        build1 = build_manager1.make_build()
        build2 = build_manager2.make_build()
        build3 = build_manager3.make_build()

        build1.add_job("job_with_no_targets", {})
        build2.add_job("job_with_no_targets", {})
        build3.add_job("job_with_no_targets", {})

        job_state1 = build1.node["job_with_no_targets"]["object"]
        job_state2 = build2.node["job_with_no_targets"]["object"]
        job_state3 = build3.node["job_with_no_targets"]["object"]

        target2 = build2.node["target"]["object"]
        target3 = build3.node["target"]["object"]

        target2.cached_mtime = True
        target2.mtime = 100
        target3.cached_mtime = True

        # When
        stale1 = job_state1.get_stale()
        stale2 = job_state2.get_stale()
        stale3 = job_state3.get_stale()

        # Then
        self.assertTrue(stale1)
        self.assertFalse(stale2)
        self.assertTrue(stale3)

    def test_meta_in_rule_dependency_graph(self):
        # Given
        job1 = builder.jobs.JobDefinition(unexpanded_id="job1")
        job2 = builder.jobs.JobDefinition(unexpanded_id="job2")
        meta = builder.jobs.MetaTarget(unexpanded_id="meta",
                                       job_collection=["job1", "job2"])

        build_manager = builder.build.BuildManager([job1, job2], [meta])
        build = build_manager.make_build()

        # When
        rule_dependency_graph = build.rule_dependency_graph

        # Then
        self.assertEqual(len(rule_dependency_graph.edge["job1"]), 1)
        self.assertEqual(len(rule_dependency_graph.edge["job2"]), 1)
        self.assertIn("meta", rule_dependency_graph)

    def test_expand_meta(self):
        # Given
        job1 = builder.jobs.JobDefinition(unexpanded_id="job1")
        job2 = builder.jobs.JobDefinition(unexpanded_id="job2")
        meta = builder.jobs.MetaTarget(unexpanded_id="meta",
                                       job_collection=["job1", "job2"])

        build_manager = builder.build.BuildManager([job1, job2], [meta])
        build = build_manager.make_build()

        # When
        build.add_meta("meta", {})

        # Then
        self.assertNotIn("meta", build)
        self.assertIn("job1", build)
        self.assertIn("job2", build)

    def test_new_nodes(self):
        # Given
        jobs = [
            builder.jobs.JobDefinition(
                "top_job",
                targets={
                    "produces": [
                        builder.expanders.Expander(
                            builder.targets.Target,
                            "top_job_target"),
                    ],
                },
                dependencies={
                    "depends": [
                        builder.expanders.Expander(
                            builder.targets.Target,
                            "top_job_depends_01"),
                        builder.expanders.Expander(
                            builder.targets.Target,
                            "top_job_depends_02"),
                    ],
                }
            ),
            builder.jobs.JobDefinition(
                "bottom_job",
                targets={
                    "produces": [
                        builder.expanders.Expander(
                            builder.targets.Target,
                            "bottom_job_target"),
                    ],
                },
                dependencies={
                    "depends": [
                        builder.expanders.Expander(
                            builder.targets.Target,
                            "top_job_target"),
                    ],
                }
            ),
        ]

        start_time = "2014-12-05T10:30"
        start_time = arrow.get(start_time)
        end_time = "2014-12-05T11:30"
        end_time = arrow.get(end_time)

        start_job1 = "top_job"
        start_job2 = "bottom_job"

        build_context1 = {
                "start_time": start_time,
                "end_time": end_time,
        }
        build_context2 = {
                "start_time": start_time,
                "end_time": end_time,
        }

        # When
        build_manager = builder.build.BuildManager(jobs, [])
        build = build_manager.make_build()

        build_update1 = build.add_job(start_job1, build_context1)
        build_update2 = build.add_job(start_job2, build_context2)

        new_nodes1 = (build_update1.new_jobs | build_update1.new_targets)
        new_nodes2 = (build_update2.new_jobs | build_update2.new_targets)

        # Then
        self.assertEqual(len(new_nodes1), 4)
        self.assertEqual(len(new_nodes2), 2)

    def test_should_run_future(self):
        # Given
        job1 = SimpleTimestampExpandedTestJob("should_run_future", file_step="5min")

        build_context1 = {
            "start_time": arrow.get("300"),
            "end_time": arrow.get("300"),
        }

        build_context2 = {
            "start_time": arrow.get("99"),
            "end_time": arrow.get("99"),
        }

        build_manager = builder.build.BuildManager([job1], [])
        build1 = build_manager.make_build()
        build2 = build_manager.make_build()

        expected_should_run1 = True
        expected_should_run2 = True

        build1.add_job("should_run_future", build_context1)
        build2.add_job("should_run_future", build_context2)


        node1 = build1.node["should_run_future_1970-01-01-00-05-00"]["object"]
        node2 = build2.node["should_run_future_1970-01-01-00-00-00"]["object"]

        node1.should_run = True
        node1.buildable = True

        node2.should_run = True
        node2.buildable = True

        old_arrow_get = copy.deepcopy(arrow.get)
        def mock_get(*args, **kwargs):
            if len(args) == 0:
                return old_arrow_get("100")
            return old_arrow_get(*args, **kwargs)

        # When
        with mock.patch("arrow.get", mock_get):

            should_run1 = node1.get_should_run()
            should_run2 = node2.get_should_run()

        self.assertEqual(should_run1, expected_should_run1)
        self.assertEqual(should_run2, expected_should_run2)

    def test_filter_target_ids(self):
        build_manager = builder.build.BuildManager([], [])
        build = build_manager.make_build()

        build.add_node(builder.targets.Target("", "target1", {}))
        build.add_node(builder.targets.Target("", "target2", {}))
        build.add_node(builder.jobs.Job(builder.jobs.JobDefinition(), "target3", {}, None))

        id_list = ["target1", "target2", "target3"]

        id_list = build.filter_target_ids(id_list)

        self.assertNotIn("target3", id_list)
        self.assertIn("target1", id_list)
        self.assertIn("target2", id_list)


    def test_expand(self):
        # Given
        target1 = builder.expanders.Expander(builder.targets.Target, "target1")
        target2 = builder.expanders.Expander(builder.targets.Target, "target2")
        target3 = builder.expanders.Expander(builder.targets.Target, "target3")
        target4 = builder.expanders.Expander(builder.targets.Target, "target4")
        target5 = builder.expanders.Expander(builder.targets.Target, "target5")
        target6 = builder.expanders.Expander(builder.targets.Target, "target6")
        target7 = builder.expanders.Expander(builder.targets.Target, "target7")
        target8 = builder.expanders.Expander(builder.targets.Target, "target8")
        target9 = builder.expanders.Expander(builder.targets.Target, "target9")
        target10 = builder.expanders.Expander(builder.targets.Target,
                                              "target10")
        target11 = builder.expanders.Expander(builder.targets.Target,
                                              "target11")
        target12 = builder.expanders.Expander(builder.targets.Target,
                                              "target12")
        target13 = builder.expanders.Expander(builder.targets.Target,
                                              "target13")
        target14 = builder.expanders.Expander(builder.targets.Target,
                                              "target14")

        job1 = JobDefinition(
            "job1",
            targets={
                "produces": [target2]
            }, dependencies={
                "depends": [target1]
            }
        )
        job2 = JobDefinition(
            "job2",
            targets={
                "produces": [target3, target4]
            }, dependencies={
                "depends": [target2]
            }
        )
        job3 = JobDefinition(
            "job3",
            targets={
                "produces": [target5]
            }, dependencies={
                "depends": [target3, target11]
            }
        )
        job4 = JobDefinition(
            "job4",
            targets={
                "produces": [target6]
            }, dependencies={
                "depends": [target4, target9]
            }
        )
        job5 = JobDefinition(
            "job5",
            targets={
                "produces": [target7]
            }, dependencies={
                "depends": [target5]
            }
        )
        job6 = JobDefinition(
            "job6",
            targets={
                "produces": [target8]
            }, dependencies={
                "depends": [target6]
            }
        )
        job7 = JobDefinition(
            "job7",
            targets={
                "produces": [target9]
            }, dependencies={
                "depends": [target10]
            }
        )
        job8 = JobDefinition(
            "job8",
            targets={
                "produces": [target11]
            }, dependencies={
                "depends": [target12]
            }
        )
        job9 = JobDefinition(
            "job9",
            targets={
                "produces": [target13]
            }, dependencies={
                "depends": [target12]
            }
        )
        job10 = JobDefinition(
            "job10",
            targets={
                "produces": [target14]
            }, dependencies={
                "depends": [target10]
            }
        )

        jobs_list = [job1, job2, job3, job4, job5, job6, job7, job8, job9,
                     job10]
        mock_jobs = []
        for job in jobs_list:
            job.expand = mock.Mock(wraps=job.expand)
            mock_jobs.append(job)
        jobs_list = mock_jobs

        build_manager = builder.build.BuildManager(jobs_list, [])
        build = build_manager.make_build()

        # When
        build.add_job("job2", {}, direction=set(["up", "down"]))

        # Then
        self.assertEqual(job1.expand.call_count, 1)
        self.assertEqual(job2.expand.call_count, 1)
        self.assertEqual(job3.expand.call_count, 1)
        self.assertEqual(job4.expand.call_count, 1)
        self.assertEqual(job5.expand.call_count, 1)
        self.assertEqual(job6.expand.call_count, 1)
        self.assertEqual(job7.expand.call_count, 1)
        self.assertEqual(job8.expand.call_count, 1)
        self.assertEqual(job9.expand.call_count, 0)
        self.assertEqual(job10.expand.call_count, 0)

    def test_job_state_iter(self):
        # Given
        job1 = SimpleTestJobDefinition(
                "job1",
                targets=["target1", "target2"],
                depends=["target3", "target4"])
        job2 = SimpleTestJobDefinition(
                "job2",
                targets=["target5", "target6"],
                depends=["target1", "target2"])
        job3 = SimpleTestJobDefinition(
                "job3",
                targets=["target7", "target8"],
                depends=["target5", "target6"])

        jobs = [job1, job2, job3]

        build_manager = builder.build.BuildManager(jobs, [])
        build = build_manager.make_build()
        build.add_job("job3", {})

        # When
        job_state_iter = build.job_iter()
        job_tuple1 = job_state_iter.next()
        job_tuple2 = job_state_iter.next()
        job_tuple3 = job_state_iter.next()
        job_tuples = [job_tuple1, job_tuple2, job_tuple3]
        job_id_matching = [(x, y.unexpanded_id) for x, y in job_tuples]

        # Then
        self.assertRaises(StopIteration, job_state_iter.next)
        self.assertNotEqual(job_tuple1, job_tuple2)
        self.assertNotEqual(job_tuple1, job_tuple3)
        self.assertNotEqual(job_tuple2, job_tuple3)
        self.assertIn(("job1", "job1"), job_id_matching)
        self.assertIn(("job2", "job2"), job_id_matching)
        self.assertIn(("job3", "job3"), job_id_matching)

    def test_cache_same_job(self):
        # Given
        job1 = SimpleTestJobDefinition(
                "job1", targets=["target1"],
                depends=["target2"])

        build_manager = builder.build.BuildManager([job1], [])
        build = build_manager.make_build()

        # When
        with mock.patch.object(builder.expanders.Expander, "expand") as \
                mock_expander:
            build.add_job("job1", {})
            build.add_job("job1", {})

        # Then
        self.assertEqual(mock_expander.call_count, 2)

    def test_get_targets(self):
        # Given
        job1 = SimpleTestJobDefinition(
            unexpanded_id="job1",
            targets=[
                {
                    "type": "alternates",
                    "unexpanded_id": "target1",
                    "ignore_mtime": True,
                },
                {
                    "type": "alternates",
                    "unexpanded_id": "target3",
                },
                {
                    "type": "produces",
                    "unexpanded_id": "target2",
                    "edge_data": {"fake": "fake"},
                },
                {
                    "type": "produces",
                    "unexpanded_id": "target4",
                },
            ]
        )

        job2 = SimpleTestJobDefinition(unexpanded_id="job2")

        build_manager = builder.build.BuildManager([job1, job2], [])
        build = build_manager.make_build()
        build.add_job("job1", {})
        build.add_job("job2", {})

        # When
        target_ids1 = build.get_target_ids("job1")
        target_ids1_iter = build.get_target_ids_iter("job1")
        target_or_ids1 = build.get_target_or_dependency_ids("job1", "down")
        target_or_ids1_iter = build.get_target_or_dependency_ids_iter("job1", "down")
        target_ids2 = build.get_target_ids("job2")
        target_ids2_iter = build.get_target_ids_iter("job2")
        target_or_ids2 = build.get_target_or_dependency_ids("job2", "down")
        target_or_ids2_iter = build.get_target_or_dependency_ids_iter("job2", "down")

        target_relationships1 = build.get_target_relationships("job1")
        target_relationships2 = build.get_target_relationships("job2")

        # Then
        self.assertEqual(len(target_ids1), 4)
        self.assertIn("target1", target_ids1)
        self.assertIn("target2", target_ids1)
        self.assertIn("target3", target_ids1)
        self.assertIn("target4", target_ids1)

        self.assertEqual(len(target_or_ids1), 4)
        self.assertIn("target1", target_or_ids1)
        self.assertIn("target2", target_or_ids1)
        self.assertIn("target3", target_or_ids1)
        self.assertIn("target4", target_or_ids1)

        self.assertEqual(len(target_ids2), 0)

        self.assertEqual(len(target_or_ids2), 0)

        count = 0
        previous_target_ids = []
        previous_target_or_ids = []
        for target_id, target_or_id in zip(target_ids1_iter, target_or_ids1_iter):
            self.assertNotIn(target_id, previous_target_ids)
            self.assertNotIn(target_id, previous_target_or_ids)
            self.assertIn(target_id, ("target1", "target2", "target3", "target4"))
            self.assertIn(target_or_id, ("target1", "target2", "target3", "target4"))
            previous_target_ids.append(target_id)
            previous_target_or_ids.append(target_or_id)
            count = count + 1

        self.assertEqual(count, 4)

        for target_id in target_ids2_iter:
            self.assertTrue(False)

        for target_id in target_or_ids2_iter:
            self.assertTrue(False)

        self.assertEqual(len(target_relationships1), 2)
        self.assertEqual(len(target_relationships2), 0)
        self.assertIn("produces", target_relationships1)
        self.assertIn("alternates", target_relationships1)

        self.assertEqual(len(target_relationships1["produces"]), 2)
        self.assertEqual(len(target_relationships1["alternates"]), 2)
        self.assertIn("target1", target_relationships1["alternates"])
        self.assertIn("target2", target_relationships1["produces"])
        self.assertIn("target3", target_relationships1["alternates"])
        self.assertIn("target4", target_relationships1["produces"])
        self.assertEqual(
            target_relationships1["alternates"]["target1"]["ignore_mtime"],
            True)
        self.assertEqual(
            target_relationships1["produces"]["target2"]["fake"],
            "fake")
        self.assertEqual(
            target_relationships1["alternates"]["target3"].get("ignore_mtime", False),
            False)
        self.assertEqual(
            target_relationships1["produces"]["target4"].get("ignore_mtime", False),
            False)


    def test_get_dependencies(self):
        # Given
        job1 = SimpleTestJobDefinition(
            unexpanded_id="job1",
            depends=[
                {
                    "type": "depends",
                    "unexpanded_id": "target1",
                    "ignore_mtime": True,
                },
                {
                    "type": "depends_one_or_more",
                    "unexpanded_id": "target2",
                    "edge_data": {"fake": "fake"},
                },
                {
                    "type": "depends",
                    "unexpanded_id": "target3",
                },
                {
                    "type": "depends_one_or_more",
                    "unexpanded_id": "target4",
                },
            ]
        )

        job2 = SimpleTestJobDefinition(unexpanded_id="job2")

        build_manager = builder.build.BuildManager([job1, job2], [])
        build = build_manager.make_build()
        build.add_job("job1", {})
        build.add_job("job2", {})

        # When
        depends_ids1 = build.get_dependency_ids("job1")
        depends_ids1_iter = build.get_dependency_ids_iter("job1")
        depends_or_ids1 = build.get_target_or_dependency_ids("job1", "up")
        depends_or_ids1_iter = build.get_target_or_dependency_ids_iter("job1", "up")
        depends_ids2 = build.get_dependency_ids("job2")
        depends_ids2_iter = build.get_dependency_ids_iter("job2")
        depends_or_ids2 = build.get_target_or_dependency_ids("job2", "up")
        depends_or_ids2_iter = build.get_target_or_dependency_ids_iter("job2", "up")
        depends_relationship1 = build.get_dependency_relationships("job1")
        depends_relationship2 = build.get_dependency_relationships("job2")

        # Then
        self.assertEqual(len(depends_ids1), 4)
        self.assertIn("target1", depends_ids1)
        self.assertIn("target2", depends_ids1)
        self.assertIn("target3", depends_ids1)
        self.assertIn("target4", depends_ids1)

        self.assertEqual(len(depends_or_ids1), 4)
        self.assertIn("target1", depends_or_ids1)
        self.assertIn("target2", depends_or_ids1)
        self.assertIn("target3", depends_or_ids1)
        self.assertIn("target4", depends_or_ids1)

        self.assertEqual(len(depends_ids2), 0)

        self.assertEqual(len(depends_or_ids2), 0)

        count = 0
        previous_depends_ids = []
        previous_depends_or_ids = []
        for target_id, depends_or_id in zip(depends_ids1_iter, depends_or_ids1_iter):
            self.assertNotIn(target_id, previous_depends_ids)
            self.assertNotIn(depends_or_id, previous_depends_or_ids)
            self.assertIn(target_id, ("target1", "target2", "target3", "target4"))
            self.assertIn(depends_or_id, ("target1", "target2", "target3", "target4"))
            previous_depends_ids.append(target_id)
            previous_depends_or_ids.append(depends_or_id)
            count = count + 1

        self.assertEqual(count, 4)

        for target_id in depends_ids2_iter:
            self.assertTrue(False)

        for target_id in depends_or_ids2_iter:
            self.assertTrue(False)

        self.assertEqual(len(depends_relationship1), 2)
        self.assertEqual(len(depends_relationship2), 0)
        self.assertIn("depends", depends_relationship1)
        self.assertIn("depends_one_or_more", depends_relationship1)
        self.assertEqual(len(depends_relationship1["depends"]), 2)
        self.assertEqual(len(depends_relationship1["depends_one_or_more"]), 2)

        target1_in = False
        target3_in = False
        for depends in depends_relationship1["depends"]:
            self.assertEqual(len(depends["targets"]), 1)
            if "target1" in depends["targets"]:
                target1_in = True
                self.assertEqual(depends["data"]["ignore_mtime"], True)
            if "target3" in depends["targets"]:
                target3_in = True
                self.assertEqual(
                    depends["data"].get("ignore_mtime", False), False)

        self.assertTrue(target1_in)
        self.assertTrue(target3_in)

        target2_in = False
        target4_in = False
        for depends in depends_relationship1["depends_one_or_more"]:
            self.assertEqual(len(depends["targets"]), 1)
            if "target2" in depends["targets"]:
                target2_in = True
                self.assertEqual(depends["data"]["fake"], "fake")
            if "target4" in depends["targets"]:
                target4_in = True
                self.assertEqual(
                    depends["data"].get("fake"), None)

        self.assertTrue(target2_in)
        self.assertTrue(target4_in)

    def test_get_creators(self):
        # Given
        job1 = SimpleTestJobDefinition(
            unexpanded_id="job1",
            targets=[
                {
                    "type": "produces",
                    "unexpanded_id": "target1",
                    "ignore_mtime": True,
                },
                {
                    "type": "produces",
                    "unexpanded_id": "target3",
                },
                {
                    "type": "alternates",
                    "unexpanded_id": "target2",
                    "edge_data": {"fake": "fake"},
                },
                {
                    "type": "alternates",
                    "unexpanded_id": "target4",
                },
            ],
        )
        job2 = SimpleTestJobDefinition(
            unexpanded_id="job2",
            targets=[
                {
                    "type": "alternates",
                    "unexpanded_id": "target1",
                },
                {
                    "type": "alternates",
                    "unexpanded_id": "target3",
                }
            ]
        )
        job3 = SimpleTestJobDefinition(
            unexpanded_id="job3",
            targets=[
                {
                    "type": "alternates",
                    "unexpanded_id": "target1",
                    "edge_data": {"fake": "fake"},
                },
                {
                    "type": "alternates",
                    "unexpanded_id": "target3",
                }
            ]
        )
        job4 = SimpleTestJobDefinition(
            unexpanded_id="job4",
            targets=[
                {
                    "type": "alternates",
                    "unexpanded_id": "target3",
                }
            ]
        )

        build_manager = builder.build.BuildManager([job1, job2, job3, job4], [])
        build = build_manager.make_build()
        build.add_job("job1", {})
        build.add_job("job2", {})
        build.add_job("job3", {})
        build.add_job("job4", {})

        # When
        creator_ids = build.get_creator_ids("target1")
        creator_ids_iter = build.get_creator_ids_iter("target1")
        creator_or_ids = build.get_dependent_or_creator_ids("target1", "up")
        creator_or_ids_iter = build.get_dependent_or_creator_ids_iter("target1", "up")
        creator_relationships = build.get_creator_relationships("target1")


        # Then
        self.assertEqual(len(creator_ids), 3)
        self.assertIn("job1", creator_ids)
        self.assertIn("job2", creator_ids)
        self.assertIn("job3", creator_ids)

        self.assertEqual(len(creator_or_ids), 3)
        self.assertIn("job1", creator_or_ids)
        self.assertIn("job2", creator_or_ids)
        self.assertIn("job3", creator_or_ids)

        count = 0
        previous_creator_ids = []
        previous_creator_or_ids = []
        for creator_id, creator_or_id in zip(creator_ids_iter, creator_or_ids_iter):
            self.assertNotIn(creator_id, previous_creator_ids)
            self.assertNotIn(creator_or_id, previous_creator_or_ids)
            self.assertIn(creator_id, ("job1", "job2", "job3"))
            self.assertIn(creator_or_id, ("job1", "job2", "job3"))
            previous_creator_ids.append(creator_id)
            previous_creator_or_ids.append(creator_or_id)
            count = count + 1

        self.assertEqual(count, 3)

        self.assertEqual(len(creator_relationships), 2)
        self.assertIn("produces", creator_relationships)
        self.assertIn("alternates", creator_relationships)
        self.assertEqual(len(creator_relationships["produces"]), 1)
        self.assertEqual(len(creator_relationships["alternates"]), 2)
        self.assertIn("job1", creator_relationships["produces"])
        self.assertIn("job2", creator_relationships["alternates"])
        self.assertIn("job3", creator_relationships["alternates"])
        self.assertEqual(creator_relationships["produces"]["job1"]["ignore_mtime"], True)
        self.assertEqual(creator_relationships["alternates"]["job2"].get("ignore_mtime",False), False)
        self.assertEqual(creator_relationships["alternates"]["job3"].get("fake"), "fake")

    def test_get_dependents(self):
        # Given
        job1 = SimpleTestJobDefinition(
            unexpanded_id="job1",
            depends=[{"type": "depends", "unexpanded_id": "target1", "ignore_mtime": True},
                     {"type": "depends_one_or_more", "unexpanded_id": "target2"}]
        )
        job2 = SimpleTestJobDefinition(
            unexpanded_id="job2",
            depends=[{"type": "depends_one_or_more", "unexpanded_id": "target1"},
                     {"type": "depends", "unexpanded_id": "target2"}]
        )
        job3 = SimpleTestJobDefinition(
            unexpanded_id="job3",
            depends=[{"type": "depends", "unexpanded_id": "target1", "edge_data": {"fake": "fake"}},
                     {"type": "depends_one_or_more", "unexpanded_id": "target2"}]
        )
        job4 = SimpleTestJobDefinition(
            unexpanded_id="job4",
            targets=[{"type": "produces", "unexpanded_id": "target3"}]
        )

        build_manager = builder.build.BuildManager([job1, job2, job3, job4], [])
        build = build_manager.make_build()
        build.add_job("job1", {})
        build.add_job("job2", {})
        build.add_job("job3", {})
        build.add_job("job4", {})

        # When
        dependent_ids1 = build.get_dependent_ids("target1")
        dependent_ids_iter1 = build.get_dependent_ids_iter("target1")
        dependent_or_ids1 = build.get_dependent_or_creator_ids("target1", "down")
        dependent_or_ids_iter1 = build.get_dependent_or_creator_ids_iter("target1", "down")
        dependent_ids2 = build.get_dependent_ids("target3")
        dependent_ids_iter2 = build.get_dependent_ids_iter("target3")
        dependent_or_ids2 = build.get_dependent_or_creator_ids("target3", "down")
        dependent_or_ids_iter2 = build.get_dependent_or_creator_ids_iter("target3", "down")
        dependent_relationships1 = build.get_dependent_relationships("target1")
        dependent_relationships2 = build.get_dependent_relationships("target3")

        # Then
        self.assertEqual(len(dependent_ids1), 3)
        self.assertIn("job1", dependent_ids1)
        self.assertIn("job2", dependent_ids1)
        self.assertIn("job3", dependent_ids1)

        self.assertEqual(len(dependent_or_ids1), 3)
        self.assertIn("job1", dependent_or_ids1)
        self.assertIn("job2", dependent_or_ids1)
        self.assertIn("job3", dependent_or_ids1)

        self.assertEqual(len(dependent_ids2), 0)
        self.assertEqual(len(dependent_or_ids2), 0)

        count = 0
        previous_dependent_ids = []
        previous_dependent_or_ids = []
        for dependent_id, dependent_or_id in zip(dependent_ids_iter1, dependent_or_ids_iter1):
            self.assertNotIn(dependent_id, previous_dependent_ids)
            self.assertNotIn(dependent_or_id, previous_dependent_or_ids)
            self.assertIn(dependent_id, ("job1", "job2", "job3"))
            self.assertIn(dependent_or_id, ("job1", "job2", "job3"))
            previous_dependent_ids.append(dependent_id)
            previous_dependent_or_ids.append(dependent_or_id)
            count = count + 1

        self.assertEqual(count, 3)

        for dependent_id in dependent_ids_iter2:
            self.assertTrue(False)
        for dependent_id in dependent_or_ids_iter2:
            self.assertTrue(False)

        self.assertEqual(len(dependent_relationships1), 2)
        self.assertEqual(len(dependent_relationships2), 0)
        self.assertIn("depends", dependent_relationships1)
        self.assertIn("depends_one_or_more", dependent_relationships1)
        self.assertEqual(len(dependent_relationships1["depends"]), 2)
        self.assertEqual(len(dependent_relationships1["depends_one_or_more"]), 1)
        self.assertEqual(dependent_relationships1["depends"]["job1"]["ignore_mtime"], True)
        self.assertEqual(dependent_relationships1["depends_one_or_more"]["job2"].get("ignore_mtime", False), False)
        self.assertEqual(dependent_relationships1["depends"]["job3"]["fake"], "fake")

    def test_should_run_failed(self):
        jobs = [
            SimpleTestJobDefinition("A", targets=["A-target1"],
                                    depends=["A-depends"])
        ]

        build_manager = builder.build.BuildManager(jobs, [])
        build = build_manager.make_build()
        build.add_job("A", {})

        job_A = build.get_job("A")
        job_A.stale = True
        job_A.buildable = True
        job_A.parents_should_run = False
        job_A.force = False
        job_A.past_curfew = mock.Mock(return_value=True)
        job_A.all_dependencies = mock.Mock(return_value=True)
        job_A.failed = True

        self.assertFalse(job_A.get_should_run())

        job_A.failed = False
        job_A.should_run = None
        self.assertTrue(job_A.get_should_run())

    def test_set_failed(self):
        jobs = [
            SimpleTestJobDefinition("A")
        ]

        build_manager = builder.build.BuildManager(jobs, [])
        build = build_manager.make_build()
        build.add_job("A", {})

        job_A = build.get_job("A")
        job_A.set_failed(True)

        self.assertTrue(job_A.failed)
        self.assertFalse(job_A.should_run)
        self.assertFalse(job_A.force)

    def test_new_and_force_nodes_from_add(self):
        # Given
        jobs = [
            SimpleTimestampExpandedTestJob(
                "job1", file_step="5min",
                targets=[
                    {
                        "unexpanded_id": "target1-%Y-%m-%d-%H-%M",
                        "file_step": "5min",
                    }
                ]
            ),
            SimpleTimestampExpandedTestJob(
                "job2", file_step="5min",
                targets=[
                    {
                        "unexpanded_id": "target2-%Y-%m-%d-%H-%M",
                        "file_step": "5min",
                    }
                ],
                depends=[
                    {
                        "unexpanded_id": "target1-%Y-%m-%d-%H-%M",
                        "file_step": "5min",
                    }
                ]
            ),
        ]

        build_manager = builder.build.BuildManager(jobs, [])
        build = build_manager.make_build()
        build_update1 = build.add_job(
            "job2", {
                "start_time": arrow.get(0),
                "end_time": arrow.get(300*2),
            }, force=False
        )

        build_update3 = build.add_job(
            "job2", {
                "start_time": arrow.get(300*3),
                "end_time": arrow.get(300*3),
            }, force=True
        )

        # When
        build_update2 = build.add_job(
            "job2", {
                "start_time": arrow.get(0),
                "end_time": arrow.get(300*4),
            }, force=True
        )


        # Then
        self.assertEqual(len(build_update1.new_jobs), 4)
        self.assertEqual(len(build_update1.new_targets), 4)
        self.assertEqual(len(build_update1.newly_forced), 0)
        self.assertIn("job1_1970-01-01-00-00-00", build_update1.new_jobs)
        self.assertIn("job2_1970-01-01-00-00-00", build_update1.new_jobs)
        self.assertIn("job1_1970-01-01-00-05-00", build_update1.new_jobs)
        self.assertIn("job2_1970-01-01-00-05-00", build_update1.new_jobs)
        self.assertIn("target1-1970-01-01-00-00", build_update1.new_targets)
        self.assertIn("target2-1970-01-01-00-00", build_update1.new_targets)
        self.assertIn("target1-1970-01-01-00-05", build_update1.new_targets)
        self.assertIn("target2-1970-01-01-00-05", build_update1.new_targets)

        self.assertEqual(len(build_update1.jobs), 4)
        self.assertEqual(len(build_update1.targets), 4)
        self.assertEqual(len(build_update1.forced), 0)
        self.assertIn("job1_1970-01-01-00-00-00", build_update1.jobs)
        self.assertIn("job2_1970-01-01-00-00-00", build_update1.jobs)
        self.assertIn("job1_1970-01-01-00-05-00", build_update1.jobs)
        self.assertIn("job2_1970-01-01-00-05-00", build_update1.jobs)
        self.assertIn("target1-1970-01-01-00-00", build_update1.targets)
        self.assertIn("target2-1970-01-01-00-00", build_update1.targets)
        self.assertIn("target1-1970-01-01-00-05", build_update1.targets)
        self.assertIn("target2-1970-01-01-00-05", build_update1.targets)

        self.assertEqual(len(build_update3.new_jobs), 2)
        self.assertEqual(len(build_update3.new_targets), 2)
        self.assertEqual(len(build_update3.newly_forced), 1)
        self.assertIn("job1_1970-01-01-00-15-00", build_update3.new_jobs)
        self.assertIn("job2_1970-01-01-00-15-00", build_update3.new_jobs)
        self.assertIn("target1-1970-01-01-00-15", build_update3.new_targets)
        self.assertIn("target2-1970-01-01-00-15", build_update3.new_targets)
        self.assertIn("job2_1970-01-01-00-15-00", build_update3.new_jobs)

        self.assertEqual(len(build_update3.jobs), 2)
        self.assertEqual(len(build_update3.targets), 2)
        self.assertEqual(len(build_update3.forced), 1)
        self.assertIn("job1_1970-01-01-00-15-00", build_update3.jobs)
        self.assertIn("job2_1970-01-01-00-15-00", build_update3.jobs)
        self.assertIn("target1-1970-01-01-00-15", build_update3.targets)
        self.assertIn("target2-1970-01-01-00-15", build_update3.targets)
        self.assertIn("job2_1970-01-01-00-15-00", build_update3.jobs)

        self.assertEqual(len(build_update2.new_jobs), 2)
        self.assertEqual(len(build_update2.new_targets), 2)
        self.assertEqual(len(build_update2.newly_forced), 3)
        self.assertIn("job1_1970-01-01-00-10-00", build_update2.new_jobs)
        self.assertIn("job2_1970-01-01-00-10-00", build_update2.new_jobs)
        self.assertIn("target1-1970-01-01-00-10", build_update2.new_targets)
        self.assertIn("target2-1970-01-01-00-10", build_update2.new_targets)
        self.assertIn("job2_1970-01-01-00-00-00", build_update2.newly_forced)
        self.assertIn("job2_1970-01-01-00-05-00", build_update2.newly_forced)
        self.assertIn("job2_1970-01-01-00-10-00", build_update2.newly_forced)

        self.assertEqual(len(build_update2.jobs), 8)
        self.assertEqual(len(build_update2.targets), 8)
        self.assertEqual(len(build_update2.forced), 4)
        self.assertIn("job1_1970-01-01-00-00-00", build_update2.jobs)
        self.assertIn("job2_1970-01-01-00-00-00", build_update2.jobs)
        self.assertIn("job1_1970-01-01-00-05-00", build_update2.jobs)
        self.assertIn("job2_1970-01-01-00-05-00", build_update2.jobs)
        self.assertIn("job1_1970-01-01-00-15-00", build_update2.jobs)
        self.assertIn("job2_1970-01-01-00-15-00", build_update2.jobs)
        self.assertIn("target1-1970-01-01-00-00", build_update2.targets)
        self.assertIn("target2-1970-01-01-00-00", build_update2.targets)
        self.assertIn("target1-1970-01-01-00-05", build_update2.targets)
        self.assertIn("target2-1970-01-01-00-05", build_update2.targets)
        self.assertIn("target1-1970-01-01-00-15", build_update2.targets)
        self.assertIn("target2-1970-01-01-00-15", build_update2.targets)
        self.assertIn("job2_1970-01-01-00-00-00", build_update2.forced)
        self.assertIn("job2_1970-01-01-00-05-00", build_update2.forced)
        self.assertIn("job2_1970-01-01-00-10-00", build_update2.forced)
        self.assertIn("job2_1970-01-01-00-15-00", build_update2.forced)

class RuleDependencyGraphTest(unittest.TestCase):

    def _get_rdg(self):
        jobs = [
            SimpleTestJobDefinition("rule_dep_construction_job_top_01",
                expander_type=builder.expanders.TimestampExpander,
                depends=[{"unexpanded_id": "rule_dep_construction_target_highest_01", "file_step": "5min"}],
                targets=[{"unexpanded_id": "rule_dep_construction_top_01", "file_step": "5min"}]
            ),
            SimpleTestJobDefinition("rule_dep_construction_job_top_02",
                expander_type=builder.expanders.TimestampExpander,
                depends=[
                    {"unexpanded_id": "rule_dep_construction_target_highest_02", "file_step": "5min"},
                    {"unexpanded_id": "rule_dep_construction_target_highest_03", "file_step": "5min"},
                    {"unexpanded_id": "rule_dep_construction_target_highest_04", "file_step": "5min"}
                ],
                targets=[
                    {"unexpanded_id": "rule_dep_construction_target_top_02", "file_step": "5min"},
                    {"unexpanded_id": "rule_dep_construction_target_top_03", "file_step": "5min"},
                    {"unexpanded_id": "rule_dep_construction_target_top_04", "file_step": "5min"}
                ]
            ),
        ]

        build_manager = builder.build.BuildManager(jobs, [])
        graph = build_manager.make_build()

        return graph.rule_dependency_graph

    def test_get_job(self):
        # Given
        graph = self._get_rdg()

        # When
        job = graph.get_job_definition('rule_dep_construction_job_top_01')

        # Then
        self.assertIsNotNone(job)


    def test_get_all_jobs(self):
        # Given
        graph = self._get_rdg()

        # When
        jobs = graph.get_all_jobs()

        # Then
        self.assertEquals(2, len(jobs))

    def test_get_all_target_expanders(self):
        # Given
        graph = self._get_rdg()

        # When
        targets = graph.get_all_target_expanders()

        # Then
        self.assertEquals(8, len(targets))
        for target in targets:
            self.assertIsInstance(target, builder.expanders.Expander)

    def test_get_job_from_meta(self):
        # Given
        meta1 = builder.jobs.MetaTarget(
                unexpanded_id="meta1",
                job_collection=["meta2", "job1"])
        meta2 = builder.jobs.MetaTarget(
                unexpanded_id="meta2",
                job_collection=["job2", "job3"])
        job1 = builder.jobs.JobDefinition(
                unexpanded_id="job1",
                targets={
                    "produces": [
                        builder.expanders.Expander(
                            builder.targets.Target,
                            unexpanded_id="target1",
                        )
                    ]
                })
        job2 = builder.jobs.JobDefinition(
                unexpanded_id="job2",
                targets={
                    "produces": [
                        builder.expanders.Expander(
                            builder.targets.Target,
                            unexpanded_id="target2",
                        )
                    ]
                })
        job3 = builder.jobs.JobDefinition(
                unexpanded_id="job3",
                targets={
                    "produces": [
                        builder.expanders.Expander(
                            builder.targets.Target,
                            unexpanded_id="target3",
                        )
                    ]
                })

        rule_dependency_graph = builder.build.RuleDependencyGraph(
                [job1, job2, job3], [meta1, meta2])

        # when
        jobs = rule_dependency_graph.get_job_ids_from_meta("meta1")

        # Then
        self.assertEqual(len(jobs), 3)
        self.assertIn("job1", jobs)
        self.assertIn("job2", jobs)
        self.assertIn("job3", jobs)

class UtilTest(unittest.TestCase):

    def test_convert_to_timedelta(self):
        truths = [
            datetime.timedelta(0, 60*5),
            datetime.timedelta(0, 60*5),
            datetime.timedelta(0, 60*5),
            datetime.timedelta(0, 60*5),
            datetime.timedelta(0, 60),
            datetime.timedelta(0, 1*5),
            datetime.timedelta(0, 1*5),
            datetime.timedelta(0, 1),
            datetime.timedelta(0, 3600*5),
            datetime.timedelta(0, 3600*5),
            datetime.timedelta(0, 3600*5),
            datetime.timedelta(0, 3600),
            datetime.timedelta(0, 86400*5),
            datetime.timedelta(0, 86400*5),
            datetime.timedelta(0, 86400),
            datetime.timedelta(-1, 79200),
            datetime.timedelta(-4),
            datetime.timedelta(-1, 86397),
            datetime.timedelta(-1, 50400),
            datetime.timedelta(-1, 85980),
            dateutil.relativedelta.relativedelta(months=+1),
            datetime.timedelta(0, 60*5),
            datetime.timedelta(0, 60*5),
        ]

        # Given
        frequencies = [
            "5T",
            "5min",
            "5 minutes",
            "5m",
            "m",
            "5s",
            "5 seconds",
            "s",
            "5h",
            "5      hours",
            "    5 hours     ",
            "h",
            "5d",
            "5 days",
            "d",
            "-2 hours",
            "-4 days",
            "-3s",
            "-10     hours",
            "-7T",
            "month",
            300,
            '300'
        ]

        # When
        converted_frequencies = [builder.util.convert_to_timedelta(freq) for freq in frequencies]

        # Then
        for truth, frequency in zip(truths, converted_frequencies):
            self.assertEquals(truth, frequency)
