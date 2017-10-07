## Release History ##

#### 1.1.0 ####

First public release to Bintray / JCenter.

#### 1.2.0 ####

The API is not binary compatible with that of 1.1.0.

1. Remove the various id-specific bitemporal factories, as all these did that wasn't already provided by `Bitemporal.withId` was to provide runtime checking of how many items matched the id.

   This is enforced anyway when client code uses a collection rendered from a bitemporal value, and such an approach won't sit well with the forthcoming subscription API, so these have been removed.

1. Subclass of `Identifiable` no longer define the `id` property concretely, so there is no need for a constructor to set up the id and backing field for it. All the is required is an abstract definition of `id` that refines the result type. This means that classes for identifiable items defined by cleint code should be abstract.