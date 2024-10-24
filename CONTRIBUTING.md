## Developer Certificate of Origin and License

By contributing to GitLab B.V., you accept and agree to the following terms and
conditions for your present and future contributions submitted to GitLab B.V.
Except for the license granted herein to GitLab B.V. and recipients of software
distributed by GitLab B.V., you reserve all right, title, and interest in and to
your Contributions.

All contributions are subject to the
[Developer Certificate of Origin and License](https://docs.gitlab.com/ee/legal/developer_certificate_of_origin).

_This notice should stay as the first item in the CONTRIBUTING.md file._

## Code of conduct

As contributors and maintainers of this project, we pledge to respect all people
who contribute through reporting issues, posting feature requests, updating
documentation, submitting pull requests or patches, and other activities.

We are committed to making participation in this project a harassment-free
experience for everyone, regardless of level of experience, gender, gender
identity and expression, sexual orientation, disability, personal appearance,
body size, race, ethnicity, age, or religion.

Examples of unacceptable behavior by participants include the use of sexual
language or imagery, derogatory comments or personal attacks, trolling, public
or private harassment, insults, or other unprofessional conduct.

Project maintainers have the right and responsibility to remove, edit, or reject
comments, commits, code, wiki edits, issues, and other contributions that are
not aligned to this Code of Conduct. Project maintainers who do not follow the
Code of Conduct may be removed from the project team.

This code of conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community.

Instances of abusive, harassing, or otherwise unacceptable behavior can be
reported by emailing contact@gitlab.com.

This Code of Conduct is adapted from the [Contributor Covenant](https://contributor-covenant.org), version 1.1.0,
available at [https://contributor-covenant.org/version/1/1/0/](https://contributor-covenant.org/version/1/1/0/).

## Style guides

See [Go standards and style guidelines](https://docs.gitlab.com/ee/development/go_guide).

## Commits

In this project we value good commit hygiene. Clean commits makes it much
easier to discover when bugs have been introduced, why changes have been made,
and what their reasoning was.

When you submit a merge request, expect the changes to be reviewed
commit-by-commit. To make it easier for the reviewer, please submit your MR
with nicely formatted commit messages and changes tied together step-by-step.

### Write small, atomic commits

Commits should be as small as possible but not smaller than required to make a
logically complete change. If you struggle to find a proper summary for your
commit message, it's a good indicator that the changes you make in this commit may
not be focused enough.

`git add -p` is useful to add only relevant changes. Often you only notice that
you require additional changes to achieve your goal when halfway through the
implementation. Use `git stash` to help you stay focused on this additional
change until you have implemented it in a separate commit.

### Split up refactors and behavioral changes

Introducing changes in behavior very often requires preliminary refactors. You
should never squash refactoring and behavioral changes into a single commit,
because that makes it very hard to spot the actual change later.

### Tell a story

When splitting up commits into small and logical changes, there will be many
interdependencies between all commits of your feature branch. If you make
changes to simply prepare another change, you should briefly mention the overall
goal that this commit is heading towards.

### Describe why you make changes, not what you change

When writing commit messages, you should typically explain why a given change is
being made. For example, if you have pondered several potential solutions, you
can explain why you settled on the specific implementation you chose. What has
changed is typically visible from the diff itself.

A good commit message answers the following questions:

- What is the current situation?
- Why does that situation need to change?
- How does your change fix that situation?
- Are there relevant resources which help further the understanding? If so,
  provide references.

You may want to set up a [message template](https://thoughtbot.com/blog/better-commit-messages-with-a-gitmessage-template)
to pre-populate your editor when executing `git commit`.

### Message format

Commit messages must be:

- Formatted following the
  [Conventional Commits 1.0](https://www.conventionalcommits.org/en/v1.0.0/)
  specification;

- Be all lower case, except for acronyms and source code identifiers;

- For localized changes, have the affected package in the scope portion, minus
  the root package prefix (`registry/`). For changes affecting multiple
  packages, use the parent package name that is common to all, unless it's the
  root one;

- Use one of the commit types defined in the [Angular convention](https://github.com/angular/angular/blob/main/CONTRIBUTING.md#type);

- For dependencies, use `build` type and `deps` scope. Include the module name
  and the target version if upgrading or adding a dependency;

- End with ` (<issue reference>)` if the commit is fixing an issue;

- Subjects shouldn't exceed 72 characters.

#### Examples

```plaintext
build(deps): upgrade cloud.google.com/go/storage to v1.16.0
```

```plaintext
fix(handlers): handle manifest not found errors gracefully (#12345)
```

```plaintext
perf(storage/driver/gcs): improve blob upload performance
```

### Mention the original commit that introduced bugs

When implementing bugfixes, it's often useful information to see why a bug was
introduced and when it was introduced. Therefore, mentioning the original commit
that introduced a given bug is recommended. You can use `git blame` or `git
bisect` to help you identify that commit.

The format used to mention commits is typically the abbreviated object ID
followed by the commit subject and the commit date. You may create an alias for
this to have it easily available. For example:

```shell
$ git config alias.reference "show -s --pretty=reference"
$ git reference HEAD
cf7f9ffe5 (style: Document best practices for commit hygiene, 2020-11-20)
```

### Use interactive rebases to arrange your commit series

Use interactive rebases to end up with commit series that are readable and
therefore also easily reviewable one-by-one. Use interactive rebases to
rearrange commits, improve their commit messages, or squash multiple commits
into one.

### Create fixup commits

When you create multiple commits as part of feature branches, you
frequently discover bugs in one of the commits you've just written. Instead of
creating a separate commit, you can easily create a fixup commit and squash it
directly into the original source of bugs via `git commit --fixup=ORIG_COMMIT`
and `git rebase --interactive --autosquash`.

### Avoid merge commits

During development other changes might be made to the target branch. These
changes might cause a conflict with your changes. Instead of merging the target
branch into your topic branch, rebase your branch onto the target
branch. Consider setting up `git rerere` to avoid resolving the same conflict
over and over again.

### Ensure that all commits build and pass tests

To keep history bisectable using `git bisect`, you should ensure that all of
your commits build and pass tests.

### Example

A great commit message could look something like:

```plaintext
fix(package): summarize change in 50 characters or less (#123)

The first line of the commit message is the summary. The summary should
start with a capital letter and not end with a period. Optionally
prepend the summary with the package name, feature, file, or piece of
the codebase where the change belongs to.

After an empty line the commit body provides a more detailed explanatory
text. This body is wrapped at 72 characters. The body can consist of
several paragraphs, each separated with a blank line.

The body explains the problem that this commit is solving. Focus on why
you are making this change as opposed to what (the code explains this).
Are there side effects or other counterintuitive consequences of
this change? Here's the place to explain them.

- Bullet points are okay, too

- Typically a hyphen or asterisk is used for the bullet, followed by a
  single space, with blank lines in between

- Use a hanging indent

These guidelines are pretty similar to those described in the Git Book
[1]. If you like you can use footnotes to include a lengthy hyperlink
that would otherwise clutter the text.

You can provide links to the related issue, or the issue that's fixed by
the change at the bottom using a trailer. A trailer is a token, without
spaces, directly followed with a colon and a value. Order of trailers
doesn't matter.

1. https://www.git-scm.com/book/en/v2/Distributed-Git-Contributing-to-a-Project#_commit_guidelines

Signed-off-by: Alice <alice@example.com>
```

## Changelog

The [`CHANGELOG.md`](CHANGELOG.md) is automatically generated using
[semantic-release](https://semantic-release.gitbook.io/semantic-release/) when
a new tag is pushed.

## Maintainers and reviewers

The list of project maintainers and reviewers can be found
[here](https://about.gitlab.com/handbook/engineering/projects/#container-registry).

Maintainers can be pinged using `@gitlab-org/maintainers/container-registry`.

## Review Process

Merge requests need **approval by at least two** members, including at least one
[maintainer](#maintainers-and-reviewers).

We use the [reviewer roulette](https://docs.gitlab.com/ee/development/code_review.html#reviewer-roulette)
to identify available reviewers and maintainers for every open merge request.
Feel free to override these selections if you think someone else would be
better-suited to review your change.

## Releases

We use [semantic-release](https://semantic-release.gitbook.io/semantic-release/)
to generate changelog entries, release commits and new git tags. A new release
is created by the project maintainers, using the `make release` command,
invoked from their local development machine. A `make release-dry-run` command
is available to anyone and allows previewing the next release.

If this is the first time you are generating a release, you must invoke the
`make dev-tools` command to install the required dependencies. This requires
having [Node.js](https://nodejs.org/en/) and [npm](https://docs.npmjs.com/cli/)
installed locally.

Once a new tag is pushed to this repository, a CI pipeline is created
([sample](https://gitlab.com/gitlab-org/container-registry/-/pipelines/713632199)).
Within the `release` stage, there are several ordered jobs that Maintainers
are responsible for triggering. These jobs are responsible for releasing in
several GitLab projects and their sequence is described in the
[Release Plan](https://gitlab.com/gitlab-org/container-registry/-/blob/master/.gitlab/issue_templates/Release%20Plan.md)
issue template. A new issue based on the same template is automatically
created as part of the CI pipeline with title
`Release Version vX.Y.Z-gitlab`.

## Golang Version Support

Please see [Supporting multiple Go versions](https://docs.gitlab.com/ee/development/go_guide/go_upgrade.html#supporting-multiple-go-versions).

Support for individual versions is ensured via the `.gitlab-ci.yml` file in the
root of this repository. If you modify this file to add additional jobs, please
ensure that those jobs run against all supported versions.

## Development Process

We follow the engineering process as described in the
[handbook](https://about.gitlab.com/handbook/engineering/workflow/),
with the exception that our
[issue tracker](https://gitlab.com/gitlab-org/container-registry/issues/) is
on the Container Registry project.

### Development Guides

- [Development Environment Setup](docs/development-environment-setup.md)
- [Local Integration Testing](docs/storage-driver-integration-testing-guide.md)
- [Offline Garbage Collection Testing](docs/garbage-collection-testing-guide.md)
- [Database Development Guidelines](docs/database-dev-guidelines.md)
- [Database Migrations](docs/database-migrations.md)
- [Feature Flags](docs/feature-flags.md)

### Technical Documentation

- [Metadata Import](docs/database-import-tool.md)
- [Push/pull Request Flow](docs/push-pull-request-flow.md)
- [Authentication Request Flow](docs/auth-request-flow.md)
- [Online Garbage Collection](docs/spec/gitlab/online-garbage-collection.md)
- [HTTP API Queries](docs/spec/gitlab/http-api-queries.md)

You can find some technical documentation inherited from the upstream Docker
Distribution Registry under [`docs`](docs), namely:

- [Configuration](docs/configuration.md) - We keep a local copy of this file
  because we extended the [upstream](https://distribution.github.io/distribution/about/configuration/)
  version with additional configuration settings over time.
- [Docker Registry HTTP API V2](docs/spec/docker/v2/api.md) - We keep a local 
  copy of this spec because we had the need to extend the
  [upstream](https://distribution.github.io/distribution/spec/api/)
  functionality.

When making changes to the HTTP API V2 or application configuration, please
make sure to always update the respective documentation linked above.

Apart from the above, you can find the following technical documentation
upstream. These are all relevant for the GitLab Container Registry:

- [Storage Drivers](https://distribution.github.io/distribution/storage-drivers/)
- [Token Authentication Specification](https://distribution.github.io/distribution/spec/auth/token/)
- [Oauth2 Token Authentication](https://distribution.github.io/distribution/spec/auth/oauth/)
- [Token Authentication Implementation](https://distribution.github.io/distribution/spec/auth/jwt/)
- [Token Scope Documentation](https://distribution.github.io/distribution/spec/auth/scope/)

### Troubleshooting

- [Cleanup Invalid Link Files](docs/cleanup-invalid-link-files.md)
