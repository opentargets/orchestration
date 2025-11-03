# Interval datasets

This docuement was update on 2025-04-11

Data source comes from the following publications:
See more information at the [gentropy documentation](https://opentargets.github.io/gentropy/python_api/datasources/intervals/_intervals/)

* Promoter Capture Hi-C (Javierre et al., 2016)
* Enhancer-TSS Correlation (Andersson et al., 2014)
* DHS-Promoter Correlation (Thurman et al., 2012)
* Promoter Capture Hi-C (Jung et al., 2019)
* Epiraction
* rE2G (ENCODE)

```{bash}
gs://interval_data/datasets/intervals.db <- duckdb database with interval datasets collapsed (not up to date)
gs://interval_data/datasets/ENCODE-rE2G/ <- original rE2G data
gs://interval_data/datasets/andersson2014/ <- original Andersson et al. data
gs://interval_data/datasets/epiractionV1.6/ <- original epiraction data
gs://interval_data/datasets/epiractionV1.6_bgzip/  <- transformed to bgzip indexed by tabix format
gs://interval_data/datasets/javierre_2016_preprocessed/
gs://interval_data/datasets/jung2019/ <- original Jung et al. data
gs://interval_data/datasets/thurman2012/ <- original Thurman et al. data
```

## Epiraction

Source data comes from the [Epiraction](https://epiraction.crg.eu/) project.
Since the source Epiraction data is available in gzipped format, the following transformations were applied:

* sorting
* bgzip compression
* indexing with tabix

The original data is available at `gs://interval_data/datasets/epiractionV1.6/` and the transformed data is available at `gs://interval_data/datasets/epiractionV1.6_bgzip/`.

## rE2G

Source data comes from the [ENCODE](https://www.encodeproject.org/) platform.
The original data is available with manifest from [query](https://www.encodeproject.org/report/?type=File&searchTerm=encode-re2g+thresholded&output_type=thresholded+element+gene+links&status%21=archived&limit=all)

The original data is available at `gs://interval_data/datasets/ENCODE-rE2G/20250704/`.
The query manifest ia available at `gs://interval_data/datasets/ENCODE-rE2G/20250704-encode-metadata.tsv`.

## Changelog

### 2025-04-11

* chore: added readme describing interval datasets

### 2025-07-22

* chore: updated readme with latest information about new interval datasets.
