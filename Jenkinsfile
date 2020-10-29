pipeline {
    agent { label "sailfish" }
    options { timestamps () }
    tools {
        jdk 'openjdk-11.0.2'
    }
    environment {
        VERSION_MAINTENANCE = """${sh(
                                    returnStdout: true,
                                    script: 'git rev-list --count VERSION-1.1..HEAD'
                                    ).trim()}""" //TODO: Calculate revision from a specific tag instead of a root commit
        TH2_SCHEMA_REGISTRY = credentials('TH2_SCHEMA_REGISTRY_USER')
        TH2_SCHEMA_REGISTRY_URL = credentials('TH2_SCHEMA_REGISTRY')
        GRADLE_SWITCHES = " --warning-mode all --stacktrace -Pversion_build=${BUILD_NUMBER} -Pversion_maintenance=${VERSION_MAINTENANCE}"
    }
    stages {
        stage ('Artifactory configuration') {
            steps {
                rtGradleDeployer(
                        id: "GRADLE_DEPLOYER",
                        serverId: "artifatory5",
                        repo: "th2-schema-snapshot-local",
                )

                rtGradleResolver(
                        id: "GRADLE_RESOLVER",
                        serverId: "artifatory5",
                        repo: "th2-schema-snapshot-local"
                )
            }
        }
        stage ('Config Build Info') {
            steps {
                rtBuildInfo (
                    captureEnv: true
                )
            }
        }
        stage('Build') {
            steps {
                rtGradleRun (
                    usesPlugin: true, // Artifactory plugin already defined in build script
                    useWrapper: true,
                    rootDir: "./",
                    buildFile: 'build.gradle',
                    tasks: "clean build artifactoryPublish ${GRADLE_SWITCHES}",
                    deployerId: "GRADLE_DEPLOYER",
                    resolverId: "GRADLE_RESOLVER",
                )
            }
        }
        stage ('Publish build info') {
            steps {
                rtPublishBuildInfo (
                    serverId: "artifatory5"
                )
            }
        }

        stage('Publish') {
            steps {
                sh """
                    docker login -u ${TH2_SCHEMA_REGISTRY_USR} -p ${TH2_SCHEMA_REGISTRY_PSW} ${TH2_SCHEMA_REGISTRY_URL}
                    ./gradlew dockerPush ${GRADLE_SWITCHES} \
                    -Ptarget_docker_repository=${TH2_SCHEMA_REGISTRY_URL}
                """ // TODO: Exec from root repository
            }
        }
        stage('Publish report') {
            steps {
                script {
                    def gradleProperties = readProperties  file: 'gradle.properties'
                    def dockerImageVersion = "${gradleProperties['version_major']}.${gradleProperties['version_minor']}.${VERSION_MAINTENANCE}.${BUILD_NUMBER}"

                    def changeLogs = ""
                    try {
                        def changeLogSets = currentBuild.changeSets
                        for (int changeLogIndex = 0; changeLogIndex < changeLogSets.size(); changeLogIndex++) {
                            def entries = changeLogSets[changeLogIndex].items
                            for (int itemIndex = 0; itemIndex < entries.length; itemIndex++) {
                                def entry = entries[itemIndex]
                                changeLogs += "\n${entry.msg}"
                            }
                        }
                    } catch(e) {
                        println "Exception occurred: ${e}"
                    }

                    currentBuild.description = "docker-image-version = ${dockerImageVersion}<br>"
                }
            }
        }
    }
}
