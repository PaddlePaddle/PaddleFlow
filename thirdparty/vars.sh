#!/bin/env

#####################################################
# Download url of all third parties
#####################################################
# volcano
# The url of volcano git
export VOLCANO_GIT_URL=https://github.com/volcano-sh/volcano.git
# The url of volcano apis git
export VOLCANO_APIS_GIT_URL=https://github.com/volcano-sh/apis.git
# The git tag of volcano
export VOLCANO_TAG=v1.3.0
# The git tag of volcano apis
export VOLCANO_APIS_TAG=21e223
# The name of the volcano patch file, located in the thirdparty/patches in the content root directory
export VOLCANO_PATCH_FILE=volcano-1.3
# The name of the volcano apis patch file, located in the thirdparty/patches in the content root directory
export VOLCANO_APIS_PATCH_FILE=volcano-apis-1.3.0-k8s1.19.6-21e223