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


The bolt data base MUST have 2 buckets: metadata and files. The former one MUST store `name` and `core`, whereas files must hold a `object:relpath` key-value relationship. The object name must be unique per package, and SHOULD have a prefix of package name.

Package bolt database spec:

```
[metadata]
name: package name
version: version
epoch: high priority version sort overrider, must be a valid uint
core: true / false
flavor: Paper/Spigot/Folia etc.
requireCore: A core flavour to require on. (Plugin only)
depends: JSON encoded dependency list
configs: JSON encoded path list

[files]
	Multiple obj: path entries (Non-core)
	Single core: path entry (Core)
```

For a server core package, the `[files]` section MUST only contain the core executable, and it must be located directly in root path. The object name MUST be `server-core`.


# Internal database format
Internal database separates package data into multiple buckets. The bucket name MUST be the same as package name. A special "core" bucket exists for the server core.

A bucket contains the following KEY=VAL pairs

```
[System]
	core=pkgname

[Bucket]
	name: pkgname
	version: version
	epoch: high priority version sort overrider, must be a valid uint
	installed: true / false
	flavor: Paper/Spigot/Folia etc.
	core: true / false
	requireCore: A core flavour to require on. (Plugin only)
	depends: JSON encoded dependency list
	configs: JSON encoded path list
	[files]
		Multiple obj: path entries (Non-core)
		Single core: path entry (Core)
```

# Package rules

The package name MUST only contain A-Z, a-z, ".", and numbers. It MUST not contain special characters and spaces.