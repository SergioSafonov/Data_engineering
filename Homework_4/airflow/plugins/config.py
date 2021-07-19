import yaml  # pip install pyyaml

# yaml.warnings({'YAMLLoadWarning': False})     # for disable deprecation warning


class Config:
    def __init__(self, path):
        with open(path, 'r') as yaml_file:
            self.__config = yaml.full_load(yaml_file)  # __config - private var
            # self.__config = yaml.load(yaml_file)  # deprecated in pip3.8

    def get_config(self, application):
        return self.__config.get(application)
