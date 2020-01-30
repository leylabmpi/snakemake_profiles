snakemake_profiles
==================

[snakemake profiles](https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles) to atapt snakemake to run in specific cluster configurations

* Version: 0.1.0
* Authors:
  * Nick Youngblut <nyoungb2@gmail.com>
* Maintainers:
  * Nick Youngblut <nyoungb2@gmail.com>

# Description

For an intro into snakemake profiles, see [the snakemake docs](https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles)

This repo only contains a couple of custom profiles that have been tested
to work with the clusters used by members of the Ley Lab. 
For more (generalized) profiles, see the [snakemake-profiles repo](https://github.com/snakemake-profiles/doc)

# Profiles

## SGE cluster

Copy the `sge` directory from this repo to `~/.config/snakemake/`:

```
$ mkdir -p ~/.config/snakemake/
$ chmod u+x sge/*.py 
$ cp -r sge ~/.config/snakemake/
```

## SLURM cluster

Copy the `slurm` directory from this repo to `~/.config/snakemake/`:

```
$ mkdir -p ~/.config/snakemake/
$ chmod u+x slurm/*.py 
$ cp -r slurm ~/.config/snakemake/
```


