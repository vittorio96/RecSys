"""In this file all the expanders are defined

A expander is used to take in a "unexpanded unique id" and the class that it
will go to and it is expanded. Expanding invloves taking the unexpanded
unique id and using a build context to create a unique id

e.x. day%d-month%m-year%Y might be expanded to day01-month12-year2015

Using these expanded id's, unique targets/jobs can be created

All base classes, e.x. StandardExpander, TimestampExpander, expand strings
These strings can be used to implement them for different file types.
"""

import copy

import builder.util as util


class Expander(object):
    """This is the base class for an expander

    All the methods in the class are abstract methods, they are also all
    the methods that an expander needs to have implemented to function with
    the api correctly.

    args:
        base_class: The class that the expand function will expand out
        unexpanded_id: The string that will be expanded to create unique ids
        edge_data: Any extra information about how the target will be
            connected to a job
        node_data: Any extra information about how the node should appear in
            the graph
        config: A dictionary of config options
        ignore_mtime: Edge data that specifies that the mtime should be
            ignored when doing a stale check
        ignore_produce: Makes the target act very similar to an alternate. This
            is used to connect a job to the target but make it so the job does
            not value the mtime or existance value of the target at all.
            Potentially should be a different type of relationship.
    """
    def __init__(self, base_class, unexpanded_id, edge_data=None,
            node_data=None, config=None, ignore_mtime=False,
            ignore_produce=False, meta=None, **kwargs):
        """An unexpanded id is one that can be turned into a unique id
        provided that the correct build context is passed

        the base class is the target or job class that the expanded id will
        be passed to
        """
        super(Expander, self).__init__()
        if edge_data is None:
            edge_data = {}
        if node_data is None:
            node_data = {}
        if config is None:
            config = {}
        if meta is None:
            meta = {}

        edge_data["ignore_mtime"] = ignore_mtime
        edge_data["ignore_produce"] = ignore_produce

        self.base_class = base_class
        self.unexpanded_id = unexpanded_id
        self.edge_data = edge_data
        self.node_data = node_data
        self.config = config
        self.meta = meta

    @staticmethod
    def expand_build_context(build_context, unexpanded_id, *args, **kwargs):
        return {unexpanded_id: build_context}

    def expand(self, build_context):
        """Returns a list of a single instance that is instantiated with the
        unexpanded id and the same build context
        """
        return [self.base_class(self.unexpanded_id,
                self.unexpanded_id, build_context) for build_context in self.expand_build_context(build_context, self.unexpanded_id).values()]

    def different(self, expander):
        """The other one should probably also check if they are different
        e.x. Expander would look to see that that id's are the same and the
             target type are the same.
             TimestampExpander would look to see that the id's are the same,
             the target types are the same and the file steps are the same.
             If timestampexpander was passed to expander, we would recieve true
             while if timestamp expander was passed to expander we would recieve
             false
        """

    def __repr__(self):
        return "Expander({}, {})".format(self.base_class, self.unexpanded_id)


class TimestampExpander(Expander):
    """This is the base for an expander that expands targets based off of
    timestamps

    implements the init function where file_step is necessary

    args:
        unexpanded_id: the id to expand with timestamps
           string in the format of a datetime.strptime
        file_step: the timestep to create the timestaps with
        past: the number of timestamps previous to start time to include
    """
    def __init__(self, base_class, unexpanded_id, file_step, past=0,
            edge_data=None, node_data=None, ignore_mtime=False,
            ignore_produce=False, config=None, meta=None):
        super(TimestampExpander, self).__init__(
                base_class, unexpanded_id, edge_data=edge_data,
                node_data=node_data, ignore_mtime=ignore_mtime,
                ignore_produce=ignore_produce,
                config=config, meta=meta)
        self.file_step = file_step
        self.past = past


    @staticmethod
    def expand_build_context(build_context, unexpanded_id, file_step,
            past=0):
        """Expands out the build_contexts and returns a dict of the form
        {
                "expanded_id": expanded_build_contex,
                "expanded_id": expanded_build_context,
                ...
        }
        """
        # Handle bgpdumps (glob target with time_step: null)
        if file_step is None:
            return {unexpanded_id: {}}

        start_time = build_context["start_time"]
        end_time = build_context.get("end_time") or start_time

        start_time = util.floor_timestamp_given_time_step(
                start_time, file_step)
        end_time = util.floor_timestamp_given_time_step(
                end_time, file_step)

        end_inclusive = False
        if end_time == start_time:
            end_inclusive = True

        if past:
            file_step_delta = util.convert_to_timedelta(file_step)
            start_time = start_time - past*file_step_delta

        timestamps = util.BuilderArrowFactory.range(
                file_step,
                start_time,
                end_time,
                end_inclusive=end_inclusive)

        new_build_context = copy.copy(build_context)
        new_build_context.pop("force", None)
        expanded_dict = {}
        for timestamp in timestamps:
            expanded_id = timestamp.strftime(unexpanded_id)

            time_delta = util.convert_to_timedelta(file_step)
            new_build_context["start_time"] = timestamp
            new_build_context["end_time"] = timestamp + time_delta
            expanded_dict[expanded_id] = copy.copy(new_build_context)
        return expanded_dict


    def expand(self, build_context):
        """Expands out the targets based on timestamps.

        using the start_time floored to the file_step as the first timestamp
        and the end_time floored to the file_step as the last timestamp
        (exclusive), the unexpanded_id is expanded and the class is
        instantiated with it.
        If start_time and end_time are equal, then the end_time is inclusive.
        Each target is given a new_build context where the start_time is equal
        to the target's timestamp and the end_time is equal to the start_time
        plus the target's file_step

        required build_context:
            start_time: floored by the file step to the first timestamp to use
            end_time: floored by the file step to the last timestamp to use
                (exclusive) if equal to start_time inclusive.
        """
        expanded_dict = self.expand_build_context(build_context,
                self.unexpanded_id, self.file_step, past=self.past)
        expanded_nodes = []
        for expanded_id, new_build_context in expanded_dict.iteritems():
            expanded_node = self.base_class(
                    self.unexpanded_id, expanded_id, new_build_context,
                    config=self.config)

            expanded_nodes.append(expanded_node)

        return expanded_nodes
