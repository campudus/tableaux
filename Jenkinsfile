IMAGE_NAME = "campudus/grud-backend"
DEPLOY_DIR = 'build/libs'
LEGACY_ARCHIVE_FILENAME="grud-backend-docker.tar.gz"
DOCKER_BASE_IMAGE_TAG = "build-${BUILD_NUMBER}"

def slackParams = { GString message, String color ->
  [
    tokenCredentialId : "${env.SLACK_GRUD_INTEGRATION_ID}",
    channel           : "#grud",
    color             : color,
    message           : message
  ]
}

def getTriggeringUser = env.BUILD_USER ? env.BUILD_USER : { sh (
      script: 'git --no-pager show -s --format=%an',
      returnStdout: true
    ).trim()
}

pipeline {
  agent any

  environment {
    GIT_HASH = sh (
      script: 'git log -1 --pretty=%h',
      returnStdout: true
    ).trim()
  }

  triggers {
    pollSCM('H/5 * * * *')
  }

  options {
    timestamps()
    copyArtifactPermission('*');
  }

  stages {
    stage('Cleanup') {
      steps {
        sh './gradlew clean'

        // ensure temp folder has write rights for tests otherwise they fail with "java.lang.IllegalStateException: Failed to create cache dir"
        sh "sudo chmod a+w /tmp/vertx-cache/"

        // cleanup docker
        sh 'docker rmi $(docker images -f "dangling=true" -q) || true'
        sh "docker rmi -f \$(docker images -qa --filter=reference='${IMAGE_NAME}') || true"
      }
    }

    stage('Assemble test classes') {
      steps {
        sh './gradlew testClasses'
      }
    }

    stage('Assemble') {
      steps {
        sh './gradlew assemble'
      }
    }

    stage('Test') {
      steps {
        script {
          try {
              configFileProvider([configFile(fileId: 'grud-backend-build', targetLocation: 'conf-test.json')]) {
                sh './gradlew test'
              }
          } finally {
            junit '**/build/test-results/test/TEST-*.xml' //make the junit test results available in any case (success & failure)
          }
        }
      }
    }

    stage('Build docker image') {
      steps {
        sh "docker build -t ${IMAGE_NAME}:${DOCKER_BASE_IMAGE_TAG}-${GIT_HASH} -t ${IMAGE_NAME}:latest -f Dockerfile --rm ."
        // Legacy, but needed for some project deployments
        sh "docker save ${IMAGE_NAME}:latest | gzip -c > ${DEPLOY_DIR}/${LEGACY_ARCHIVE_FILENAME}"
      }
    }

    stage('Archive artifacts') {
      steps {
        archiveArtifacts artifacts: "${DEPLOY_DIR}/*-fat.jar", fingerprint: true
        archiveArtifacts artifacts: "${DEPLOY_DIR}/${LEGACY_ARCHIVE_FILENAME}", fingerprint: true
      }
    }

    stage('Push to docker registry') {
      steps {
        withDockerRegistry([ credentialsId: "dockerhub", url: "" ]) {
          sh "docker push ${IMAGE_NAME}:${DOCKER_BASE_IMAGE_TAG}-${GIT_HASH}"
          sh "docker push ${IMAGE_NAME}:latest"
        }
      }
    }
  }

  post {
    success {
      wrap([$class: 'BuildUser']) {
        script {
          sh "echo successful"
          slackSend(slackParams("""Build successful: <${BUILD_URL}|${env.JOB_NAME} @ \
              ${env.BUILD_NUMBER}> (${getTriggeringUser()})""", "good"))
        }
      }
    }

    failure {
      wrap([$class: 'BuildUser']) {
        script {
          sh "echo failed"
          slackSend(slackParams("""Build failed: <${BUILD_URL}|${env.JOB_NAME} @ \
              ${env.BUILD_NUMBER}> (${getTriggeringUser()})""", "danger"))
        }
      }
    }
  }
}
