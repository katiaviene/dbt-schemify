from setuptools import setup, find_packages
from setuptools.command.install import install
import os
from dbt_schemify.dbt_schemify.generate import generate_default_schema, node_to_dict, write_schema
from pathlib import Path

class PostInstallCommand(install):
    def run(self):
        # Run the default install process
        install.run(self)
        # Call the function to create the .schemify.yml file after the package is installed
        self.create_default_schema()

    def create_default_schema(self):
        # Define the path for .schemify.yml file
        schemify_file = '.schemify.yml'

        if not os.path.exists(schemify_file):  # Check if the file already exists
            default_node = generate_default_schema()
            default_dict = node_to_dict(default_node)
            write_schema(schemify_file, default_dict)



setup(
    name='dbt-schemify',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pyyaml',  # or any dependencies your package requires
    ],
    cmdclass={
        'install': PostInstallCommand,
    },
)
