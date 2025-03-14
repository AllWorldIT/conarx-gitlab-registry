# Style Guide

We strive to adhere to GitLab [Go standards and style guidelines](https://docs.gitlab.com/ee/development/go_guide/) wherever possible. This document outlines style decisions that deviate from or are omitted from the general guidelines.

## Configuration Parameters

While the majority of the registry [configuration](./configuration.md) parameters use flat case notation (all lowercase, no spaces or special characters), exceptions exist. Given that flat case can decrease readability and increase the likelihood of typos, we use snake case notation for all _new_ configuration parameters.

### Environment Variables

The current configuration parser has fundamental limitations that prevent it from [handling environment variable overrides](./configuration.md#override-specific-configuration-options) when using snake case notation in the field names. See [this issue comment](https://gitlab.com/gitlab-org/container-registry/-/issues/1464#note_2393643851) for detailed explanation. We've planned a [configuration parser refactoring](https://gitlab.com/gitlab-org/container-registry/-/issues/1536) to address this. Until then, for any new settings with snake case notation, we must manually lookup their corresponding environment variables and override the respective settings _after_ the configuration has been parsed.

#### Example

For example, if dealing with the following new settings:

```yaml
foo_bar:
  password: "secret"
```

We'll need to manually look up `REGISTRY_FOO_BAR_PASSWORD` and, if set, use its value to override the `secret` value that was parsed from the configuration file.
