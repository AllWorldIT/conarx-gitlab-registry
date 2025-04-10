# Feature Flags

To toggle features we can make use of internally defined feature flags, on the [`feature`](https://gitlab.com/gitlab-org/container-registry/-/tree/master/registry/internal/feature)
package. This is done by relying on environment variables, which can then be set independently across environments.

## Create new feature flag

To create a new feature flag, add the feature definition to the `feature` package, like so:

```go
// LightningDownloads is used to toggle the experimental lightning speed downloads feature. Proceed with caution.
var LightningDownloads = Feature{
    EnvVariable: "REGISTRY_FF_LIGHTNING_DOWNLOADS",
}
```

Please follow the `REGISTRY_FF_<NAME>` naming convention for consistency.

Once defined, you can then call `feature.<FF variable>.Enabled()` in the relevant parts of the application to determine
if the feature should be enabled or not.

The feature flag definition should be introduced in the same merge request used to introduce the actual feature code.

Feature flags are disabled by default. If necessary, a feature flag can be enabled by default by setting its `defaultEnabled` attribute to `true`. However, the recommended approach is to always start with disabled by default and rely on explicitly enabling them in the deployment environment configuration. Nevertheless, the functionality exists in case it becomes necessary. We should follow the best practices defined in the corresponding [GitLab development documentation](https://docs.gitlab.com/ee/development/feature_flags/#feature-flags-in-gitlab-development) as much as possible.

## Toggle feature flags

For GitLab.com, it is possible to define a specific environment variable by adding an entry for it under the `extraEnv`
section of the registry Helm chart template for each environment, like so:

```yaml
registry:
  extraEnv:
   REGISTRY_FF_LIGHTNING_DOWNLOADS: "true"
```

This can be done independently for all environments and their stages. To do so we need to create an MR for the
[k8s-workloads](https://gitlab.com/gitlab-com/gl-infra/k8s-workloads/gitlab-com) project and update the `.yaml.gotmpl`
that corresponds to the desired environment. All files can be found
[here](https://gitlab.com/gitlab-com/gl-infra/k8s-workloads/gitlab-com/-/tree/master/releases/gitlab/values). Feature
flags should be toggled individually for each environment, and for production canary and main stages (in this order),
all with enough time between to assert that the corresponding functionality is functioning properly.

## Remove feature flags

Once the feature flag is no longer required, remove its definition from `feature` package along with the source code
that toggles the feature on the corresponding application code.

If the feature flag's corresponding environment variable was injected into the registry through the `extraEnv` parameter for Helm deployments (as discussed above), then also make sure to remove those injected environment variables from the deployment configurations for each environment after the necessary cleanup has been completed on the application code and the registry has been re-deployed. This would guarantee that we do not amass a huge number of unused environment variables in our deployment configurations.
