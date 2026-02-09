# Minepak package format

The minepak package format consists of a single file, with the file extension of `.minepak`. It should be a gzip compressed archive, of which has the following structure:

```
top.kimiblock.minepak.package/
|
|
|- info/
   |-- metadata.bolt (A bolt database which stores all files and metadata)
|- object/
   |-- ... (Files meant to be managed by the package manager)
```


The bolt data base MUST have 2 buckets: metadata and files. The former one MUST store `name` and `core`, whereas files must hold a `object:relpath` key-value relationship.


# Internal database format
Internal database separates package data into multiple buckets. The bucket name MUST be the same as package name. A special "core" bucket exists for the server core.

A bucket contains the following KEY=VAL pairs

```
[Bucket]
name: pkgname
flavor: Paper/Spigot/Folia etc. (Core only)
requireCore: A core flavour to require on. (Plugin only)
depends: JSON encoded dependency list
config: JSON encoded path list
objname: relative path to the managed file, could be more than 1
```