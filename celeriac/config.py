#!/usr/bin/env python
import runpy


class Config(object):
    get_task_instance = None
    is_flow = None
    edge_table = None
    failures = None
    nowait_nodes = None
    propagate_parent = None
    propagate_finished = None
    output_schemas = None

    storage_mapping = None
    task_mapping = None

    @classmethod
    def _set_config(cls, config_module):
        cls.get_task_instance = config_module['get_task_instance']
        cls.is_flow = config_module['is_flow']
        cls.edge_table = config_module['edge_table']
        cls.failures = config_module['failures']
        cls.nowait_nodes = config_module['nowait_nodes']
        cls.propagate_parent = config_module['propagate_parent']
        cls.propagate_finished = config_module['propagate_finished']
        cls.output_schemas = config_module['output_schemas']
        cls.storage_mapping = config_module['storage2instance_mapping']
        cls.task_mapping = config_module['task2storage_mapping']

    @classmethod
    def set_config_py(cls, config_code):
        """
        Set dispatcher configuration by Python config file
        :param config_code: configuration source code
        """
        config_module = runpy.run_path(config_code)
        cls._set_config(config_module)

    @classmethod
    def set_config_yaml(cls, nodes_definition_file, flow_definition_files):
        """
        Set dispatcher configuration by path to YAML configuration files
        :param nodes_definition_file: definition of system nodes - YAML configuration
        :param flow_definition_files: list of flow definition files
        """
        # TODO: add once Parsley will be available in pip; the implementation should be (not tested):
        # import tempfile
        # from parsley import System
        # system = System.from_files(nodes_definition_file, flow_definition_files, no_check=False)
        # # we could do this in memory, but runpy does not support this
        # tmp_file = tempfile.NamedTemporaryFile(mode="rw")
        # system.dump2stream(tmp_file)
        # cls.set_config_code(tmp_file.name)
        raise NotImplementedError()

