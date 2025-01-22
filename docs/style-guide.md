## Style Guide

We strive to adhere to GitLab [Go standards and style guidelines](https://docs.gitlab.com/ee/development/go_guide/) wherever possible. This document outlines style decisions that deviate from or are omitted from the general guidelines.

### Configuration Parameters

While the majority of the registry [configuration](./configuration.md) parameters use flat case notation (all lowercase, no spaces or special characters), exceptions exist. Given that flat case can decrease readability and increase the likelihood of typos, we use snake case notation for all _new_ configuration parameters.
