# Implementation plan

Necessary steps to be able to read a table definition from disk.

## Implement `PartitionSpec`
This also includes `SortOrder`

The partition spec is part of the manifest files. It has one or many source column ids and a partition field id. 
In addition a transform can be defined and it has a partition name.

Source column ids:
* primitives
