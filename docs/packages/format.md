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