"""The graph file holds logic on how to build out the rule dependency graph
and the build graph
"""

import collections
import os
import tempfile
import subprocess
import time
import logging

import networkx

import builder.dependencies
import builder.jobs
import builder.targets

LOG = logging.getLogger(__name__)

class BuildUpdate(object):
    """Used to contain the results of a build update.

    Add job and add meta will return this

    attr:
        new_jobs: job nodes that were added to the graph
        new_targets: targets nodes that were added to the graph
        newly_forced: jobs that are forced that either were not part of the
            graph before or that were not forced before
        jobs: jobs that were part of the expansion
        targets: targets that were part of the expansion
        forced: jobs that are forced
    """
    def __init__(self, new_jobs=None, new_targets=None, newly_forced=None,
                 jobs=None, targets=None, forced=None):
        if new_jobs is None:
            new_jobs = set()
        if new_targets is None:
            new_targets = set()
        if newly_forced is None:
            newly_forced = set()
        if jobs is None:
            jobs = set()
        if targets is None:
            targets = set()
        if forced is None:
            forced = set()

        self.new_jobs = new_jobs
        self.new_targets = new_targets
        self.newly_forced = newly_forced
        self.jobs = jobs
        self.targets = targets
        self.forced = forced

    def merge(self, build_update):
        self.new_jobs = self.new_jobs | build_update.new_jobs
        self.new_targets = self.new_targets | build_update.new_targets
        self.newly_forced = self.newly_forced | build_update.newly_forced
        self.jobs = self.jobs | build_update.jobs
        self.targets = self.targets | build_update.targets
        self.forced = self.forced | build_update.forced

class BuildManager(object):
    """A build manager holds a rule dependency graph and is then creates a new
    build graph by recieving a list of start jobs and a build_context

    A build manager is usefull when looking to creating separate build graphs
    using the same rule dependency graph.
    """

    dependency_registery = {
        "depends": builder.dependencies.depends,
        "depends_one_or_more": builder.dependencies.depends_one_or_more,
    }

    def __init__(self, jobs, metas=None, dependency_registery=None, config=None):
        super(BuildManager, self).__init__()
        if metas is None:
            metas = []
        if dependency_registery is None:
            dependency_registery = BuildManager.dependency_registery
        if config is None:
            config = {}

        self.jobs = jobs
        self.metas = metas
        self.dependency_registery = dependency_registery
        self.config = config

        self.rule_dependency_graph = RuleDependencyGraph(jobs, metas,
                                                         config=config)

    def make_build(self):
        """Constructs a new build graph by adding the jobs and following the
        build_context's rules
        """
        build_graph = BuildGraph(
                self.rule_dependency_graph,
                dependency_registery=self.dependency_registery,
                config=self.config)
        return build_graph

    def get_rule_dependency_graph(self):
        """ Return the rule dependency graph that drives all builds by this BuildManager
        """
        return self.rule_dependency_graph


class BaseGraph(networkx.DiGraph):

    def write_dot(self, file_name):
        """Writes the rule dependency graph to the file_name

        Currently does not modify the graph in anyway before writing out
        """
        networkx.write_dot(self, file_name)

    def write_pdf(self, file_name):
        """
        Writes the rule dependency graph to file_name
        """
        with tempfile.NamedTemporaryFile() as f:
            self.write_dot(f.name)
            dot = '/usr/bin/dot' if os.path.exists('/usr/bin/dot') else 'dot'
            subprocess.check_call([dot, '-Tpdf', f.name, file_name])

class RuleDependencyGraph(BaseGraph):
    """The rule dependency graph holds all the information on how jobs relate
    to jobs and their targets. It also holds information on what their aliases
    are
    """
    def __init__(self, jobs, metas, config=None):
        super(RuleDependencyGraph, self).__init__()
        if config is None:
            config = {}

        if metas is None:
            metas = []

        self.jobs = jobs
        self.metas = metas
        self.config = config
        self.construct()

    def add_node(self, node, attr_dict=None, **attr):
        """Add a job instance, expander instance, or meta node to the graph

        The node is added as the "object" keyword to the node. Some defaults are
        given to the node but can be overwritten by attr_dict, anything in attr
        overwrites that

        Args:
            node: The object to add to the "object" value
            attr_dict: Node attributes, same as attr_dict for a normal networkx
                graph overwrites anything that is defaulted, can even overwrite
                node
            attr: overwrites anything that is defaulted, can even overwrite node
                and attr_dict
        """
        if attr_dict is None:
            attr_dict = {}

        node_data = {}
        node_data["object"] = node
        # targets get special coloring
        if isinstance(node, builder.expanders.Expander):
            node_data["style"] = "filled"
            node_data["fillcolor"] = "#C2FFFF"
            node_data["color"] = "blue"

        if isinstance(node, builder.jobs.MetaTarget):
            node_data["style"] = "filled"
            node_data["fillcolor"] = "#FFE0FF"
            node_data["color"] = "purple"

        node_data.update(attr)
        node_data.update(attr_dict)

        super(RuleDependencyGraph, self).add_node(node.unexpanded_id,
                                                  attr_dict=node_data)

    def add_meta(self, meta):
        """Adds a meta target into the rule dependency graph

        Using the meta passed in, a meta node and an edge between the meta node
        and all of it's jobs specified in it's job collection are added to the
        graph. The resulting node has a meta instance as it's object and is
        connected to the nodes specified by the job collection.

        Args:
            meta: the meta target to add to the rule dependnecy graph
        """
        self.add_node(meta)
        jobs = meta.get_job_collection(self)
        for job in jobs:
            self.add_edge(job, meta.unexpanded_id, label="meta")

    def add_job_definition(self, job):
        """Adds a job definition and it's targets and dependencies to the rule dependency
        graph

        Using the job class passed in, a job node, and nodes for each of the
        targets specified by get_target_ids and get_dependency_ids is added. The
        resulting nodes have a job as the job node's object and a expander as
        the target nodes' object.

        Args:
            job: the job to add to the rule dependency graph
        """
        self.add_node(job)
        targets = job.get_targets()
        for target_type, target in targets.iteritems():
            for sub_target in target:
                self.add_node(sub_target)
                self.add_edge(job.unexpanded_id, sub_target.unexpanded_id,
                              label=target_type)

        dependencies = job.get_dependencies()
        for dependency_type, dependency in dependencies.iteritems():
            for sub_dependency in dependency:
                self.add_node(sub_dependency)
                self.add_edge(sub_dependency.unexpanded_id, job.unexpanded_id,
                              label=dependency_type)

    def construct(self):
        """Constructs the rule dependency graph.

        Adds all the jobs that are specified by the jobs keyword to the graph
        """
        for job in self.jobs:
            if not job.get_enabled():
                continue
            self.add_job_definition(job)

        for meta in self.metas:
            if not meta.get_enabled():
                continue
            self.add_meta(meta)

    def is_meta(self, meta_id):
        """Returns if the id passed in relates to a meta node"""
        meta = self.node[meta_id]
        if "object" not in meta:
            return False
        if not isinstance(meta["object"], builder.jobs.MetaTarget):
            return False
        return True

    def assert_meta(self, meta_id):
        """Asserts that the id is a meta node"""
        if not self.is_meta(meta_id):
            raise RuntimeError("{} is not a meta node".format(meta_id))

    def is_job_definition(self, job_id):
        """Returns if the id passed in relates to a job node"""
        job = self.node[job_id]
        if "object" not in job:
            return False
        if not isinstance(job["object"], builder.jobs.JobDefinition):
            return False
        return True

    def assert_job_definition(self, job_id):
        """Raises a runtime error if the job_id doesn't correspond to a job.

        Checks the node with id job_id and then raises and error if there is no
        object in the node or the object is not a job

        Args:
            job_id: the id of the node to check

        Returns:
            None

        Raises:
            RuntimeError: raised if the node specified is not a job node
        """
        if not self.is_job_definition(job_id):
            raise RuntimeError("{} is not a job node".format(job_id))

    def is_target_expander(self, target_id):
        """Returns if the id passed in relates to a target node or not"""
        target = self.node[target_id]
        if "object" not in target:
            return False
        if not isinstance(target["object"], builder.expanders.Expander):
            return False
        return True

    def assert_target_expander(self, target_id):
        """Raises a runtime error if the target_id doesn't correspond to a
        target

        Checks the node with id target_id and then raises an error if there is
        no object in the node or the object is not an Expander

        Args:
            target_id: the id of the node to check

        Returns:
            None

        Raises:
            RuntimeError: raised if the node specified is not a target node
        """
        if not self.is_target_expander(target_id):
            raise RuntimeError("{} is not a target node".format(target_id))

    def filter_target_ids(self, target_ids):
        """Takes in a list of ids in the graph and returns a list of the ids
        that correspond to targets

        An id is considered to be a target id if the object in the node
        specified by the id is an instance of Expander

        Args:
            target_ids: A list of ids that are potentially targets

        Returns:
            A filtered list of target_ids where only the id's corresponding to
            target nodes are left.
        """
        output_target_ids = []
        for target_id in target_ids:
            if self.is_target_expander(target_id):
                output_target_ids.append(target_id)
        return output_target_ids

    def filter_job_ids(self, job_ids):
        """Takes in a list of ids in the graph and returns a list of ids that
        correspond to jobs

        An id is considered to be a job id if the object in the node specified
        by the id is an instance of Job

        Args:
            job_ids: A list of ids that are potentially jobs

        Returns:
            A filtered list of job_ids where only the id's corresponding to job
            nodes are left.
        """
        output_job_ids = []
        for job_id in job_ids:
            if self.is_job_definition(job_id):
                output_job_ids.append(job_id)
        return output_job_ids

    def get_target_ids(self, job_id):
        """Returns a list of the ids of all the targets for the job_id

        The targets for the job_id are the target nodes that are direct
        decendants of job_id

        Args:
            job_id: The job to return the targets of

        Returns:
            A list of ids corresponding to the targets of job_id
        """
        self.assert_job_definition(job_id)
        neighbor_ids = self.neighbors(job_id)
        return self.filter_target_ids(neighbor_ids)

    def get_dependency_ids(self, job_id):
        """Returns a list of the ids of all the dependency targets for the
        job_id

        The dependencies for the job_id are the target nodes that are direct
        predecessors of job_id

        Args:
            job_id: The job to return the targets of

        Returns:
            A list of ids corresponding to the dependencies of job_id
        """
        self.assert_job_definition(job_id)
        target_ids = self.predecessors(job_id)
        dependency_target_ids = self.filter_target_ids(target_ids)
        return dependency_target_ids

    def get_creators(self, target_id):
        """Returns a list of the ids of all the creators for the target_id

        The creators of a target are all direct predecessors of the target

        Args:
            target_id: The target_id to return the creators of

        Returns:
            A list of ids corresponding to the creators of the target_id
        """
        self.assert_target_expander(target_id)
        parent_ids = self.predecessors(target_id)
        return self.filter_job_ids(parent_ids)

    def get_dependents(self, target_id):
        """Returns a list of the ids of all the dependents for the target_ids

        The dependents of a target are all the direct decendants of the target

        Args:
            target_id: The target_id to return the dependents of

        Returns:
            A list of ids corresponding to the dependents of the target_id
        """
        self.assert_target_expander(target_id)
        job_ids = self.neighbors(target_id)
        dependent_ids = self.filter_job_ids(job_ids)
        return dependent_ids

    def get_dependents_or_creators(self, target_id, direction):
        """Returns the dependents or the creators of the targets depending on
        the direction

        direction can be up (creators) down (dependents)

        Args:
            target_id: the target to return the dependents or creators of
            direction: The direction that the returned nodes will be to the
                target_id
        """
        if direction == "up":
            return self.get_creators(target_id)
        else:
            return self.get_dependents(target_id)

    def get_meta(self, meta_id):
        """Returns the object corresponding to the meta_id"""
        self.assert_meta(meta_id)
        return self.node[meta_id]["object"]

    def get_target_expander(self, target_id):
        """Returns the object corresponding to the target_id"""
        self.assert_target_expander(target_id)
        return self.node[target_id]["object"]

    def get_job_definition(self, job_definition_id):
        """Returns the object corresponding to the job_id

        The object corresponding to the job_id is the object keyword of the node
        with the id job_id

        Args:
            job_id: the id of the node holding the job

        Returns:
            the object in the object keyword for the node corresponding to
            job_id
        """
        self.assert_job_definition(job_definition_id)
        return self.node.get(job_definition_id, {}).get('object')

    def get_all_jobs(self):
        """Return a list of all jobs in the rule dependency graph
        """
        jobs = []
        for job_id in filter(lambda x: self.is_job_definition(x), self.node):
            jobs.append(self.get_job_definition(job_id))

        return jobs

    def get_all_target_expanders(self):
        """Return a list of all jobs in the rule dependency graph
        """
        targets = []

        def select_nodes(node):
            data = node.get('object')
            if isinstance(data, builder.expanders.Expander) and issubclass(data.base_class, builder.targets.Target):
                return True

            return False

        for target_node in filter(select_nodes, self.node.itervalues()):
            targets.append(target_node['object'])

        return targets

    def get_job_ids_from_meta(self, meta_id):
        """Returns job ids for the meta, different from job collection as metas
        can point to other metas"""
        job_ids = []
        meta = self.get_meta(meta_id)
        job_collection = meta.get_job_collection(self)
        for job_id in job_collection:
            if self.is_meta(job_id):
                job_ids = job_ids + self.get_job_ids_from_meta(job_id)
            else:
                self.assert_job_definition(job_id)
                job_ids.append(job_id)

        return job_ids

class BuildGraph(BaseGraph):
    """The build object will control the rule dependency graph and the
    build graph"""
    def __init__(self, rule_dependency_graph, dependency_registery=None, config=None):
        super(BuildGraph, self).__init__()
        if dependency_registery is None:
            dependency_registery = {}
        if config is None:
            config = {}

        self.rule_dependency_graph = rule_dependency_graph
        self.dependency_registery = dependency_registery
        self.config = config

    def add_node(self, node, build_update=None, attr_dict=None, **kwargs):
        """Adds a job, target, dependency node to the graph

        A node is added to the graph where the object keyword of the node will
        be node and the other keywords will be defined by the defaults, kwargs,
        and attr_dict. The id of the added node is defined by the unique id of
        node.

        If the node already is in the graph, then the new node data updates the
        data of the old node.

        Args:
            node: the node to add to the build_graph
            attr_dict: a dict of node data, will overwrite the default values.
                Can even overwrite the object value
            kwrags: the remaining attributes are considered to be node data.
                Will overwrite the default values. Can also overwrite attr_dict
                and the object value

        Returns:
            Returns the nodes that is now in the graph
        """
        if attr_dict is None:
            attr_dict = {}

        node_data = {}
        if node.unique_id in self:
            node_data = self.node[node.unique_id]
            node = self.node[node.unique_id]["object"]

        if isinstance(node, builder.targets.Target):
            node_data["style"] = "filled"
            node_data["fillcolor"] = "#C2FFFF"
            node_data["color"] = "blue"
        node_data.update(attr_dict)
        node_data.update(kwargs)
        node_data["object"] = node

        if build_update is not None:
            if node.unique_id not in self:
                if self.is_job_object(node):
                    build_update.new_jobs.add(node.unique_id)
                elif self.is_target_object(node):
                    build_update.new_targets.add(node.unique_id)
            if self.is_job_object(node):
                build_update.jobs.add(node.unique_id)
            elif self.is_target_object(node):
                build_update.targets.add(node.unique_id)

        super(BuildGraph, self).add_node(node.unique_id, attr_dict=node_data)
        node = self.node[node.unique_id]["object"]
        return node

    def is_dependency_type_object(self, dependency_type):
        """Returns true if the object passed in is a dependnecy type object"""
        return isinstance(dependency_type, builder.dependencies.Dependency)

    def is_dependency_type(self, dependency_id):
        """Returns if the ndoe relating to dependnecy id is a dependency node"""
        dependency_container = self.node[dependency_id]
        if "object" not in dependency_container:
            return False
        if not self.is_dependency_type_object(dependency_container["object"]):
            return False
        return True

    def assert_dependency_type(self, dependency_id):
        if not self.is_dependency_type(dependency_id):
            raise RuntimeError("{} is not a depends node".format(dependency_id))

    def is_target_object(self, target):
        """Returns true if the object is a target"""
        return isinstance(target, builder.targets.Target)

    def is_target(self, target_id):
        """Returns if the node related to target_id is a target node"""
        target = self.node[target_id]
        if "object" not in target:
            return False
        if not self.is_target_object(target["object"]):
            return False
        return True

    def assert_target(self, target_id):
        """Raises a runtime error if the target_id doesn't correspond to a
        target

        Checks the node with id target_id and then raises an error if there is
        no object in the node or the object is not an Target

        Args:
            target_id: the id of the node that should be a Target node

        Returns:
            None

        Raises:
            RuntimeError: raised if the node specified is not a Target node
        """
        if not self.is_target(target_id):
            raise RuntimeError("{} is not a target node".format(target_id))

    def get_target_ids_iter(self, job_id):
        """Returns an iter of all the target ids of the job"""
        self.assert_job(job_id)
        for target_id in self.neighbors_iter(job_id):
            if self.is_target(target_id):
                yield target_id

    def get_target_ids(self, job_id):
        """Returns a list of all the target ids of the job"""
        return list(self.get_target_ids_iter(job_id))

    def get_dependency_ids_iter(self, job_id):
        """Returns an iter of target ids that the job is dependent on"""
        self.assert_job(job_id)
        for depends_id in self.predecessors_iter(job_id):
            if self.is_dependency_type(depends_id):
                for dependency_id in self.predecessors_iter(depends_id):
                    if self.is_target(dependency_id):
                        yield dependency_id

    def get_dependency_ids(self, job_id):
        """Returns a list of target ids that the job is dependent on"""
        return list(self.get_dependency_ids_iter(job_id))

    def get_creator_ids_iter(self, target_id):
        """Returns an iter of job ids that are creators of the target.

        Note:
            A creator is different than a producer. Creators include alternates,
            etc.
        """
        self.assert_target(target_id)
        for creator_id in self.predecessors_iter(target_id):
            if self.is_job(creator_id):
                yield creator_id

    def get_creator_ids(self, target_id):
        """Returns a list of job ids that are creators of the target

        Note:
            A creator is different than a producer. Creators include alternates,
            etc.
        """
        return list(self.get_creator_ids_iter(target_id))

    def get_dependent_ids_iter(self, target_id):
        """Returns an iter of job ids that are dependent on the target"""
        self.assert_target(target_id)
        for depends_id in self.neighbors_iter(target_id):
            if self.is_dependency_type(depends_id):
                for dependent_id in self.neighbors_iter(depends_id):
                    if self.is_job(dependent_id):
                        yield dependent_id

    def get_dependent_ids(self, target_id):
        """Returns a list of job ids that are dependent on the target"""
        return list(self.get_dependent_ids_iter(target_id))

    def get_target_or_dependency_ids_iter(self, job_id, direction):
        """Returns an iter of all the dependency or target ids depending on
        direction

        Args:
            target_id: The id of the target to get the dependency or target ids
               for
            direction: "up" or "down", "up" returns dependencies and "down" returns
                targets
        """
        if direction == "up":
            return self.get_dependency_ids_iter(job_id)
        else:
            return self.get_target_ids_iter(job_id)

    def get_target_or_dependency_ids(self, job_id, direction):
        """Returns a list of all the dependency or target ids depending on
        direction

        Args:
            target_id: The id of the target to get the dependency or target ids
               for
            direction: "up" or "down", "up" returns dependencies and "down" returns
                targets
        """
        return list(self.get_target_or_dependency_ids_iter(job_id,
                                                           direction))

    def get_dependent_or_creator_ids_iter(self, target_id, direction):
        """Returns an iter of all the dependent or creator ids depending on
        direction

        Args:
            target_id: The id of the target to get the dependent or creator ids
               for
            direction: "up" or "down", "up" returns creators and "down" returns
                dependents
        """
        if direction == "up":
            return self.get_creator_ids_iter(target_id)
        else:
            return self.get_dependent_ids_iter(target_id)

    def get_dependent_or_creator_ids(self, target_id, direction):
        """Returns a list of all the dependent or creator ids depending on
        direction

        Args:
            target_id: The id of the target to get the dependent or creator ids
               for
            direction: "up" or "down", "up" returns creators and "down" returns
                dependents
        """
        return list(self.get_dependent_or_creator_ids_iter(target_id,
                                                           direction))

    def get_target_relationships(self, job_id):
        """Returns the target relationship dict for the job

        Returns:
            The target relationship dict of the form
            {
                "produces": {
                    "target_id1": {
                        "ignore_mtime": True,
                        ...
                    },
                    "target_id2": { ... },
                    ...
                },
                "alternates": { ... },
                ...
            }
        """
        self.assert_job(job_id)
        out_edges_iter = self.out_edges_iter(job_id, data=True)
        target_dict = collections.defaultdict(dict)
        for _, target_id, data in out_edges_iter:
            if self.is_target(target_id):
                target_dict[data["kind"]][target_id] = data
        return target_dict

    def get_dependency_relationships(self, job_id):
        """Returns the dependency relationship dict for the job

        Returns:
            The dependency relationship dict of the form
            {
                "depends": [
                    {
                        "targets": [
                            "dependency_target_id1",
                            "dependency_target_id2",
                            ...
                        ]
                        "data": {
                            "ignore_mtime": True,
                            ...
                        }
                    },
                    ...
                ],
                "depends_one_or_more": [ ... ],
                ...
            }
        """
        self.assert_job(job_id)
        in_edges_iter = self.in_edges_iter(job_id, data=True)
        dependency_dict = collections.defaultdict(list)
        for depends_node_id, _, data in in_edges_iter:
            if self.is_dependency_type(depends_node_id):
                depends_node = self.get_dependency_type(depends_node_id)
                group_dict = {}
                group_dict["data"] = data
                group_dict["targets"] = self.filter_target_ids(
                        self.predecessors(depends_node_id))
                dependency_dict[depends_node.kind].append(group_dict)
        return dependency_dict

    def get_creator_relationships(self, target_id):
        """Returns the creator relationship dict of the target

        Returns:
            The creator relationship dict of the form
            {
                "produces": {
                    "producing_job_id1": {
                        "ignore_mtime": True,
                        ...
                    },
                    "producing_job_id2": { ... },
                    ...
                },
                "alternates": { ... },
                ...
            }
        """
        self.assert_target(target_id)
        in_edges_iter = self.in_edges_iter(target_id, data=True)
        creator_dict = collections.defaultdict(dict)
        for creator_id, _, data in in_edges_iter:
            if self.is_job(creator_id):
                creator_dict[data["kind"]][creator_id] = data
        return creator_dict

    def get_dependent_relationships(self, target_id):
        """Returns the dependent relationship dict for the target

        Returns:
            The dependent relationship dict of form
            {
                "depends": {
                    "dependent_job_id1": {
                        "ignore_mtime": True,
                        ...
                    },
                    "dependent_job_id2": { ... },
                    ...
                },
                "depends_one_or_more": { ... },
                ...
            }
        """
        self.assert_target(target_id)
        out_edges = self.out_edges_iter(target_id)
        dependent_dict = collections.defaultdict(dict)
        for _, depends_id in out_edges:
            if self.is_dependency_type(depends_id):
                dependent_edges = self.out_edges_iter(depends_id, data=True)
                for _, dependent_id, data in dependent_edges:
                    if self.is_job(dependent_id):
                        dependent_dict[data["kind"]][dependent_id] = data
        return dependent_dict


    def get_dependents_or_creators_iter(self, target_id, direction):
        """Takes in a target id and returns an iterator for either the dependent
        ids or the creator ids depending on the direcion

        Args:
            target_id: the target id to get the dependent ids or the creator ids
                for
            direction: If the direction is "up" the creators are retrieved and
                if it is "down" then the dependents are retrieved.
                Must be "up" or "down" raises a value error if it is
                neither.

        Returns:
            An iterator for either the dependency ids or the target ids of the
            job depending on the the direction.
        """
        if direction == "up":
            return self.get_creator_ids_iter(target_id)
        elif direction == "down":
            return self.get_dependent_ids_iter(target_id)
        else:
            raise ValueError("direction must be up or down, recieved "
                             "{}".format(direction))

    def _connect_targets(self, node, target_type, targets, edge_data,
                         build_update):
        """Connets the node to it's targets

        All the targets are connected to the node. The corresponding edge data
        is what is given by edge_data and the label is target_type

        Args:
            node: the node that the targets are targets for.
            target_type: the type of the targets (produces, alternates, ...) and
                the label for the edge
            targets: all the targets that should be connected to the node.
            edge_data: any extra data to be added to the edge dict
        """
        for target in targets:
            target = self.add_node(target, build_update)
            self.add_edge(node.unique_id, target.unique_id, edge_data,
                          label=target_type, kind=target_type)

    def _connect_dependencies(self, node, dependency_type, dependencies, data,
                              build_update):
        """Connets the node to it's dependnecies

        All the depenencies are connected to the node. The corresponding edge
        data is what is given by data and the label is dependency_type.

        A depends node is put inbetween the job node and the dependencies. The
        type of depends node is looked up with the id dependency_type

        Args:
            node: the node that the dependencies are dependencies for.
            dependency_type: the type of dependency. Looked up to create the
                depends node. Is also the label for the edge
            dependencies: The nodes that shoulds be connected to the node
            data: any extra data to be added to the edge dict
        """
        dependency_node_id = "{}_{}_{}".format(
            node.unique_id, dependency_type.func_name,
            "_".join([x.unique_id for x in dependencies]))

        dependency = builder.dependencies.Dependency(dependency_type,
                                                     dependency_node_id,
                                                     dependency_type.func_name)

        # self.add_node(dependency, build_update, label=dependency_type.func_name)
        self.add_node(dependency, build_update)

        self.add_edge(dependency_node_id, node.unique_id, data,
                      label=dependency_type.func_name,
                      kind=dependency_type.func_name)

        for dependency in dependencies:
            dependency = self.add_node(dependency, build_update)
            self.add_edge(dependency.unique_id, dependency_node_id, data,
                          label=dependency_type.func_name,
                          kind=dependency_type.func_name)

    def _expand_direction(self, job, direction, build_update):
        """Takes in a node and expands it's targets or dependencies and adds
        them to the graph

        The taregets are expanded if direction is down and the dependencies are
        expanded if the direction is up

        Args:
            node: the node that need's it's targets or dependnecies expanded for
            direction: the direction that the expanded nodes are in realtion to
                the node
        """
        # The node has already been expanded in that direction
        if job.expanded_directions[direction]:
            target_ids = self.get_target_or_dependency_ids(job.unique_id, direction)
            build_update.targets.update(target_ids)
            return [self.get_target(x) for x in target_ids]
        job.expanded_directions[direction] = True

        # get the list of targets or dependencies to expand
        target_depends = {}
        unexpanded_job = self.rule_dependency_graph.get_job_definition(job.unexpanded_id)
        if direction == "up":
            target_depends = unexpanded_job.get_dependencies()
        else:
            target_depends = unexpanded_job.get_targets()

        expanded_targets_list = []
        # expanded for each type of target or dependency
        for target_type, target_group in target_depends.iteritems():
            for target in target_group:
                build_context = job.build_context
                edge_data = target.edge_data
                expanded_targets = target.expand(build_context)
                if direction == "up":
                    dependency_type = self.dependency_registery[target_type]
                    self._connect_dependencies(job, dependency_type,
                                               expanded_targets, edge_data,
                                               build_update)

                if direction == "down":
                    self._connect_targets(job, target_type, expanded_targets,
                                          edge_data, build_update)
                expanded_targets_list = expanded_targets_list + expanded_targets
        return expanded_targets_list

    def _self_expand_next_direction(self, expanded_directions, depth,
                                    current_depth, build_update, cache_set,
                                    direction, directions_to_recurse):
        """Expands out the next job nodes

        Args:
            expanded_directions: Eithe the list of the dependencies or the
                targets of the current node
            depth: How far the graph should be expanded in any branch
            current_depth: The depth the branch has been expanded
            cache_set: A set of jobs that have already been expanded
            direction: The direction that the next nodes sould be in relation to
                the current
        """
        next_nodes = []
        for expanded_direction in expanded_directions:
            if expanded_direction.unique_id in cache_set:
                continue

            # if the node is already in the graph, then return the nodes in the
            # direction of direction
            if expanded_direction.expanded_directions[direction]:
                next_node_ids = self.get_dependent_or_creator_ids(
                        expanded_direction.unique_id, direction)
                for next_node_id in next_node_ids:
                    next_nodes.append(self.get_job(next_node_id))
                continue

            # we have to use the unexpanded node to look in the rule dependency
            # graph for the next job
            unexpanded_next_node_ids = (
                    self.rule_dependency_graph
                        .get_dependents_or_creators(
                                expanded_direction.unexpanded_id, direction))

            # expand out the job and then add it to a list so that they can
            # continue the expansion later
            for unexpanded_next_node_id in unexpanded_next_node_ids:
                unexpanded_next_node = self.rule_dependency_graph.get_job_definition(
                        unexpanded_next_node_id)
                next_nodes = next_nodes + unexpanded_next_node.expand(self, expanded_direction.build_context)
            cache_set.add(expanded_direction.unique_id)
            expanded_direction.expanded_directions[direction] = True

        # continue expanding in the direction given
        for next_node in next_nodes:
            self._self_expand(next_node, directions_to_recurse, depth, current_depth,
                              build_update, cache_set)
        return next_nodes


    def _self_expand(self, job, direction, depth, current_depth, build_update, cache_set):
        """Input a node to expand and a build_context, magic ensues

        The node should already be an expanded node. It then expands out the
        graph in the direction given in relation to the node.

        Args:
            node: the expanded node to continue the expansion of the graph in
            direction: the direction to expand in the graph
            depth: the maximum depth that any branch should be
            current_depth: the depth that the branch is in
            build_update: the BuildUpdate to hold all the values relating to the
                current update
        """
        if job.unique_id in cache_set:
            return


        job = self.add_node(job, build_update)

        expanded_targets = self._expand_direction(job, "down", build_update)
        expanded_dependencies = self._expand_direction(job, "up", build_update)
        cache_set.add(job.unique_id)

        current_depth = current_depth + 1
        if depth is not None:
            if current_depth >= depth:
                return

        expanded_nodes = []
        if "up" in direction:
            new_direction = set(["up"])
            expanded_nodes += self._self_expand_next_direction(
                    expanded_dependencies, depth, current_depth, build_update,
                    cache_set, "up", new_direction)
        if "down" in direction:
            expanded_nodes += self._self_expand_next_direction(
                    expanded_targets, depth, current_depth, build_update,
                    cache_set, "down", direction)
        return expanded_nodes


    def add_meta(self, new_meta, build_context, direction=None, depth=None,
                 force=False):
        """Adds in a specific meta and expands it using the expansion strategy

        Args:
            new_meta: the meta to add to the graph

            All the rest of the args are forwarded onto add_job

        Returns:
            A list of ids of nodes that are new to the graph during the adding
            of this new meta
        """
        jobs = self.rule_dependency_graph.get_job_ids_from_meta(new_meta)
        build_update = BuildUpdate()
        for job in jobs:
            build_update.merge(self.add_job(job, build_context,
                                            direction=direction,
                                            depth=depth, force=force))

        return build_update


    def add_job(self, job_definition_id, build_context, direction=None, depth=None,
                force=False):
        """Adds in a specific job and expands it using the expansion strategy

        Args:
            job_definition_id: the id of the job_definition to add to the build graph
            build_context: the context to expand this job out for
            direction: the direction to expand the graph
            depth: the number of job nodes deep to expand
            force: whether or not to force the new job

        Returns:
            A list of ids of nodes that are new to the graph during the adding
            of this new job
        """
        LOG.debug("Adding job from job definition {} with build context {}".format(job_definition_id, build_context))
        LOG.debug("Adding job with depth {}".format(depth))
        if direction is None:
            direction = set(["up"])


        start_job = self.rule_dependency_graph.get_job_definition(job_definition_id)
        expanded_jobs = start_job.expand(self, build_context)


        current_depth = 0
        cache_set = set()

        start = time.time()
        build_update = BuildUpdate()
        cache_set = set()
        for expanded_job in expanded_jobs:
            self._self_expand(expanded_job, direction, depth, current_depth,
                              build_update, cache_set)
            if force:
                job_id = expanded_job.get_id()
                job = self.get_job(job_id)
                if not job.get_force():
                    build_update.newly_forced.add(job_id)
                build_update.forced.add(job_id)
                job.set_force(True)
        stop = time.time()
        LOG.debug("It took {} seconds to expand the build graph".format((stop - start)))
        return build_update


    def get_target(self, target_id):
        """
        Fetch target with the given ID
        """
        return self.node[target_id]["object"]

    def get_input_target_iter(self):
        for target_id, target in self.target_iter():
            if len(self.get_creator_ids(target_id)) == 0:
                yield target_id, target

    def get_input_target_ids(self):
        return [target_id for target_id, target in self.get_input_target_iter()]

    def get_job_definition(self, job_definition_id):
        """
        Fetch job with the given ID
        """
        return self.rule_dependency_graph.get_job_definition(job_definition_id)

    def is_job_object(self, job):
        """Returns true if the job object passed is a job"""
        return isinstance(job, builder.jobs.Job)

    def is_job(self, job_id):
        """Returns if the id passed in relates to a job node"""
        if not job_id in self.node:
            raise KeyError("Job '{}' is not in build graph".format(job_id))
        job_container = self.node[job_id]
        if "object" not in job_container:
            return False
        if not self.is_job_object(job_container["object"]):
            return False
        return True

    def filter_target_ids(self, target_ids):
        """Takes in a list of target ids and returns a list containing the ids
        that correspond to a target
        """
        return [x for x in target_ids if self.is_target(x)]

    def assert_job(self, job_id):
        """Asserts it is a job"""
        if not self.is_job(job_id):
            raise RuntimeError(
                    "{} is not a job node".format(job_id))

    def get_job(self, job_id):
        """
        Fetch job with the given ID
        """
        self.assert_job(job_id)
        return self.node[job_id]['object']

    def get_dependency_type(self, dependency_type_id):
        self.assert_dependency_type(dependency_type_id)
        return self.node[dependency_type_id]["object"]

    def job_iter(self):
        """Returns an iterator over the graph's (job_id, job)
        pairs
        """
        for node_id in self.node:
            if self.is_job(node_id):
                yield node_id, self.get_job(node_id)

    def target_iter(self):
        """Returns an iterator over the graph's (target_id, target) pairs
        """
        for node_id in self.node:
            if self.is_target(node_id):
                yield node_id, self.get_target(node_id)

    def bulk_refresh_targets(self, uncached_only=True):
        """
        Refresh all target existences in bulk.

        uncached_only: Only refresh targets that have no cached value for existence
        """
        LOG.debug("Bulk refreshing existence")
        type_target_map = collections.defaultdict(list)
        for target_id, target in self.target_iter():
            type_target_map[type(target)].append(target)

        touched_target_ids = []
        for target_type, targets in type_target_map.iteritems():
            LOG.debug("Refreshing {} targets of type {}".format(len(targets), target_type))
            exists_map = target_type.get_bulk_exists_mtime(targets)
            for target_id, state in exists_map.iteritems():
                target = self.get_target(target_id)
                if target.is_cached() and uncached_only:
                    continue
                touched_target_ids.append(target_id)
                target.set_mtime(state['mtime'])
