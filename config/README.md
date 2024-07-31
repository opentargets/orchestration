# Config description

This is the config documentation for the airflow dag configs.

- [x] filter.csv - file representing 500 GWAS Catalog studies for testing purposes
- [x] ot_config.yaml - configuration for existing open targets genetics dags
- [x] datasets/ and step/ - detailed configuration of the genetics dags including step configuration and Google Cloud paths configuration
- [x] config.yaml - configuration for the GWAS Catalog DAG (in progress) TODO: change the name to match the dag name.

The overall idea is to merge existing configurations into single-file configurations that will be dynamically fetched from the `root/config/` directory given the dag name. (IN progress)
