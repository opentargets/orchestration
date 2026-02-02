# GnomAD data

This document was updated on 2026-01-30.

Open targets utilizes two datasets comming from [gnomAD downloads](https://gnomad.broadinstitute.org/data).
These include:

- gnomAD LD matrices v2.1.1 [Linkage disequilibrium](https://gnomad.broadinstitute.org/data#v2-linkage-disequilibrium)
- gnomAD variant_index (based on [gnomAD variants](https://gnomad.broadinstitute.org/data#v4-variants))

Open targets ingest both datasets in `gnomad_ingestion` dag.

The output of these steps is saved to `gs://gnomad_data_2` bucket.

```{bash}
gs://gnomad_data_2/v2.1.1/ld_index/
gs://gnomad_data_2/v4.1/variant_index/
gs://gnomad_data_2/grch37_to_grch38.over.chain
```

![gnomad_ingestion](gnomad_ingestion.svg)

## Changelog

### 2025-04-11

- chore: add documentation of gnomad_ingestion dag.

### 2026-01-30

- [fix: regenerated gnomad LDIndex dataset post LD index liftover fix](https://github.com/opentargets/issues/issues/4195)
