library 'ci-packages-v4'
customLibrary 'data-ci-packages'

pipeline {
    agent {
        kubernetes {
            yamlMergeStrategy merge()
            inheritFrom 'Kaniko Krane'
            defaultContainer 'poetry'
            yaml('''
            spec:
                containers:
                    - name: poetry
                      image: 127793779807.dkr.ecr.us-east-1.amazonaws.com/python-poetry-builder:3.12
                      command: ["sleep", "infinity"]
                      resources:
                      limits:
                          cpu: 2
                          memory: 2Gi
                      requests:
                          cpu: 50m
                          memory: 512Mi
            ''')
        }
    }
    options {
        disableConcurrentBuilds()
        timeout(time: 30, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '15'))
    }

    stages {
        stage('Install dependencies') {
            steps {
                script {
                    poetry.install()
                }
            }
        }

        stage('Linting') {
            steps {
                script {
                    poetry.lint('data_slacklake', 'tests')
                }
            }
        }

        stage('Tests') {
            steps {
                script {
                    poetry.test()
                }
            }
        }

        stage('Build Image') {
            steps {
                script {
                    def extraArgsParam = '--use-new-run --snapshotMode=redo --tarPath image.tar --customPlatform linux/amd64'

                    kaniko.buildNoPush(
                        imageName: getImageName(),
                        imageTag: getImageTag(),
                        extraArgs: extraArgsParam
                    )
                }
            }
        }

        stage('Push Image') {
            when {
                expression { isDeployBranch() }
            }
            steps {
                script {
                    krane.push(
                        path: 'image.tar',
                        imageName: getImageName(),
                        imageTag: getImageTag()
                    )
                }
            }
        }

        stage('Update Lambda Handler') {
            when {
                expression { isDeployBranch() }
            }
            options {
                skipDefaultCheckout true
            }
            agent {
                kubernetes {
                    inheritFrom 'AwsCli'
                    defaultContainer 'aws-cli'
                    yamlMergeStrategy merge()
                    yaml k8s.yamlForServiceAccount(targetAccount())
                }
            }
            steps {
                script {
                    awsLambda.updateFunctionSourceImage(
                        functionName: getFunctionName(),
                        region: getRegion(),
                        registry:  getRegistry(),
                        imageName: getImageName(),
                        imageTag: getImageTag()
                    )
                    awsLambda.updateFunctionHandler(
                        functionName: getFunctionName(),
                        region: getRegion(),
                        functionHandler: getFunctionHandler()
                    )
                }
            }
        }
    }

    post {
        always {
            script {
                if (gitRef.isMain()) {
                    slack.notifyResult()
                }
            }
        }
    }
}

def isDeployBranch() {
  return (gitRef.isMaster() || env.BRANCH_NAME == "dev")
}

def getImageName() {
    return "lambda-slacklake"
}

def getRegion() {
    return 'us-east-1'
}

def getFunctionHandler() {
    return 'main.handler'
}

def getImageTag() {
    def prefix = gitRef.isMain() ? 'prod' : gitRef.realBranchName()
    return utils.sanitizeImageTag("${prefix}-${gitRef()}")
}

def getRegistry() {
    def region = getRegion()
    def sharedAccount = aws.getAccountId(aws.SHARED)
    return "${sharedAccount}.dkr.ecr.${region}.amazonaws.com"
}

String getFunctionName() {
    def prefix = gitRef.isMain() ? 'prod' : gitRef.realBranchName()
    def regionAlias = aws.aliasForRegion(getRegion())
    def serviceName = 'data-slacklake'
    def functionName = 'bot'
    return "${prefix}-${regionAlias}-${serviceName}-${functionName}"
}

def targetAccount() {
  if (gitRef.isMaster()) {
    return aws.PRODUCTION
  }
  return aws.DEVELOPMENT
}