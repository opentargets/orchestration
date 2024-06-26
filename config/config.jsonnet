// this is the small jsonnet exaple of the full DAG configuration


local gwas_catalog_data_bucket = "gs://gwas_catalog_data";
local gwas_catalog_release_ftp ="ftp://ftp.ebi/ac.uk/pub/databases/gwas/releases/latest/";

{
  config: {
    dataproc: {
      spark_uri: "yarn",
      write_mode: "errorifexists"
    },
    googlebatch: {},
    common_params: {
      gwas_catalog_data_manifests: std.format("%s/manifests", gwas_catalog_data_bucket),
      gwas_catalog_data_curated_inputs: std.format("%s/curated_inputs", gwas_catalog_data_bucket)
    },
    DAGS: [
      {
        jobs: {
          name: "prepare_curation_manifests",
          params: {
            inputs: {
              gwas_catalog_release_ftp: gwas_catalog_release_ftp,
              gwas_catalog_manifest_files: [
                std.format("%s/%s", [gwas_catalog_release_ftp, file]) for file in 
                [
                  "gwas-catalog-associations_ontology-annotated.tsv",
                  "gwas-catalog-download-studies-v1.0.3.1.txt",
                  "gwas-catalog-unpublished-studies-v1.0.3.1.tsv",
                  "gwas-catalog-download-ancestries-v1.0.3.1.txt",
                  "gwas-catalog-unpublished-ancestries-v1.0.3.1.tsv"
                ]
              ],
              
            } // inputs end

            
          }
        }
      }
    ]
  }
}