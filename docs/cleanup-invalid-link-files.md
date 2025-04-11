# Cleanup Invalid Link Files

When an invalid link file is detected, a warning will appear in the garbage
collector output.

```shell
$ registry garbage-collect /path/to/config.yml -m
...
myorg/myproj/myimg
WARN[0000] invalid link file, ignoring                   go.version=go1.13.4 instance.id=ee7a40cc-6166-4dbf-af7a-0547bf89fae1 path="/docker/registry/v2/repositories/myorg/myproj/myimg/_manifests/revisions/sha256/e4355b66995c96b4b468159fc5c7e3540fcef961189ca13fee877798649f531a/link"
myorg/myproj/myimg2
myorg/myproj/myimg2: marking manifest sha256:ae9dd3bbe42bf13bc318af4af2842b323465312392b96d44893895e8a0438565 
myorg/myproj/myimg2: marking blob sha256:a49ff3e0d85f0b60ddf225db3c134ed1735a3385d9cc617457b21875673da2f0
...
```

In the example above, while scanning the `myorg/myproj/myimg` image, the
garbage collector found an invalid link file for revision
`e4355b66995c96b4b468159fc5c7e3540fcef961189ca13fee877798649f531a`.

Each of these log messages has a `path` attribute, whose value is the full
path to the corresponding invalid file.

## Parsing the Log and Deleting

The log can be parsed as follows to obtain a list with the full path of the
invalid link files:

```shell
cat /path/to/garbage_collector.log | sed -n 's/^.*invalid link file.*path="\(\S*\)"$/\1/p' > invalid_files.txt
```

The command above will generate a new file, named `invalid_files.txt`,
containing a line with the full path to each invalid link file.

The files can then be deleted by iterating over the list. As an example, for
the filesystem driver it could be done as follows:

```shell
xargs rm < invalid_files.txt
```
