How to use AWS CLI from Knowledge Items
=======================================

If you are dealing with AWS and have a HIRO instance you naturally want to use the power of AWS CLI in HIRO.

Below we describe one approach to this can be accomplished


## Preparation in AWS

To use the AWS API (through CLI) you need credentials. This is usually done by
with the following steps

* create a new policy in AWS IAM which cover all the operations you wan to allow your HIRO instance to perform
* create new (technical) user in AWS IAM
* assign the policy to that user
* create an access key for that user and download access key and secret (to be used later)

more detail can be found on https://aws.amazon.com/documentation/iam/

## Preparation on HIRO Engine host I: enable AWS CLI

* Install AWS CLI as described here: http://docs.aws.amazon.com/cli/latest/userguide/installing.html
* switch user to 'arago' (run-user of HIRO engine) and then configure AWS CLI is described on http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-quick-configuration:
** enter access key and secret from previous step
** set "Default output format" to `json`
* test if 'aws' command is working for user 'arago' 


##  Preparation on HIRO Engine host II: configure action handler

Add the following secitont to section `GenericHandler:` of file `/opt/autopilot/conf/aae.yaml` (depending on how AWS CLI was installed the path the `aws` may be different) :

```yaml
- Applicability:
  - Priority: 10
    ModelFilter:
      Truefilter:
  Capability:
  - Name: AWS
    Description: "run command against AWS"
    Command: /opt/autopilot/.local/bin/aws ${GlobalOpts} ${Command}
    Parameter:
    - Name: Command
      Description: "contains the subcommand with a service and all options and arguments"
      Mandatory: True
    - Name: GlobalOpts
      Description: "override some defauls, like region, profile"
      Mandatory: false
```

## Sample usage KnowledgeItem

