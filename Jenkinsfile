@Library('campudus-jenkins-shared-lib') _

IMAGE_NAME = "campudus/grud-backend"
DOCKER_WORKDIR = "/usr/src/app"
DEPLOY_DIR = 'build/libs'
TEST_RESULTS_DIR = 'build/test-results'
LEGACY_ARCHIVE_FILENAME="grud-backend-docker.tar.gz"
DOCKER_BASE_IMAGE_TAG = "build-${BUILD_NUMBER}"

// SLACK_CHANNEL = "#grud"
SLACK_CHANNEL = "@mz"

pipeline {
  agent any

  environment {
    GIT_HASH = sh (script: 'git log -1 --pretty=%H', returnStdout: true).trim()
    BUILD_DATE = sh(script: 'date \"+%Y-%m-%dT%H:%M:%S\"', returnStdout: true).trim()
    // `git show -s --format=%cI` doesn't work because of very old git version
    GIT_COMMIT_DATE = sh(script: "date -d @\$(git log -n1 --format='%at') +%Y-%m-%dT%H:%M:%S", returnStdout: true).trim()
    CLEAN_GIT_BRANCH = sh(script: "echo $GIT_BRANCH | sed 's/[\\.\\/\\_\\#]/-/g'", returnStdout: true).trim()
  }

  triggers {
    pollSCM('H/5 * * * *')
  }

  options {
    timestamps()
  }

  stages {
    stage('Cleanup') {
      steps {
        // clean up build folder
        sh 'rm -rf ./build'

        echo "Environment variables are:"
        sh "printenv | sort"
        echo ""
        echo "Groovy/Pipeline variables are:"
        script {
            groovyVars = [:] << getBinding().getVariables()
            groovyVars.each  {k,v -> print "$k = $v"}
        }

        // cleanup docker
        sh 'docker rmi $(docker images -f "dangling=true" -q) || true'
        sh "docker rmi -f \$(docker images -qa --filter=reference='${IMAGE_NAME}-builder') || true"
        sh "docker rmi -f \$(docker images -qa --filter=reference='${IMAGE_NAME}-tester') || true"

        // create folder
        sh "mkdir -p ./build"
      }
    }

    stage('Assemble') {
      steps {
        script {
          try {
            sh """
              docker build \
              --build-arg GIT_BRANCH --build-arg GIT_COMMIT \
              --build-arg GIT_COMMIT_DATE --build-arg BUILD_DATE \
              -t ${IMAGE_NAME}-builder:build-${BUILD_NUMBER} \
              --target=builder .
            """
          } finally {
            sh "docker cp \$(docker create --name temp-builder ${IMAGE_NAME}-builder:build-${BUILD_NUMBER}):${DOCKER_WORKDIR}/${DEPLOY_DIR} ./build && docker rm temp-builder"
          }
        }
      }
    }

    stage('Test') {
      steps {
        script {
          try {
            configFileProvider([configFile(fileId: 'grud-backend-build-dockerized', targetLocation: 'conf-test.json')]) {
              sh """
                docker build \
                -t ${IMAGE_NAME}-tester:build-${BUILD_NUMBER} \
                --target=tester .
              """
            }
          } finally {
            sh "docker cp \$(docker create --name temp-tester ${IMAGE_NAME}-tester:build-${BUILD_NUMBER}):${DOCKER_WORKDIR}/${TEST_RESULTS_DIR} ./build && docker rm temp-tester"
            junit '**/build/test-results/test/TEST-*.xml' //make the junit test results available in any case (success & failure)
          }
        }
      }
    }

    stage('Build docker image') {
      steps {
        echo "skipped"
      //   sh "docker build -t ${IMAGE_NAME}:${DOCKER_BASE_IMAGE_TAG}-${GIT_HASH} -t ${IMAGE_NAME}:latest -f Dockerfile --rm ."
      //   // Legacy, but needed for some project deployments
      //   sh "docker save ${IMAGE_NAME}:latest | gzip -c > ${DEPLOY_DIR}/${LEGACY_ARCHIVE_FILENAME}"
      }
    }

    stage('Archive artifacts') {
      steps {
        sh "ls -rtl"
        sh "ls -rtl ${DEPLOY_DIR}"
        // archiveArtifacts artifacts: "${DEPLOY_DIR}/*-fat.jar", fingerprint: true
        // archiveArtifacts artifacts: "${DEPLOY_DIR}/${LEGACY_ARCHIVE_FILENAME}", fingerprint: true
      }
    }

    stage('Push to docker registry') {
      steps {
        echo "skipped"
        // withDockerRegistry([ credentialsId: "dockerhub", url: "" ]) {
        //   sh "docker push ${IMAGE_NAME}:${DOCKER_BASE_IMAGE_TAG}-${GIT_HASH}"
        //   sh "docker push ${IMAGE_NAME}:latest"
        // }
      }
    }
  }

  post {
    success {
      wrap([$class: 'BuildUser']) {
        script {
          slackOk(channel: SLACK_CHANNEL)
        }
      }
    }

    failure {
      wrap([$class: 'BuildUser']) {
        script {
          slackError(channel: SLACK_CHANNEL)
        }
      }
    }
  }
}
