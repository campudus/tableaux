IMAGE_NAME = "grud-backend"
DEPLOY_DIR = 'build/libs'
ARCHIVE_FILENAME_DOCKER="${IMAGE_NAME}-docker.tar.gz"

def slackParams = { GString message, String color ->
  [
    tokenCredentialId : "${env.SLACK_GRUD_INTEGRATION_ID}",
    channel           : "#grud",
    color             : color,
    message           : message
  ]
}

pipeline {
  agent any

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


    stage('Build docker image') {
      steps {
        sh "docker build -t ${IMAGE_NAME} -f Dockerfile --rm ."
        sh "docker save ${IMAGE_NAME} | gzip -c > ${DEPLOY_DIR}/${ARCHIVE_FILENAME_DOCKER}"
      }
    }

    stage('Archive artifacts') {
      steps {
        archiveArtifacts artifacts: "${DEPLOY_DIR}/*-fat.jar", fingerprint: true
        archiveArtifacts artifacts: "${DEPLOY_DIR}/${ARCHIVE_FILENAME_DOCKER}", fingerprint: true
      }
    }
  }

  post {
    success {
      wrap([$class: 'BuildUser']) {
        script {
          sh "echo successful"
          slackSend(slackParams("Build successful: ${env.JOB_NAME} @ ${env.BUILD_NUMBER} (${BUILD_USER})", "good"))
        }
      }
    }

    failure {
      wrap([$class: 'BuildUser']) {
        script {
          sh "echo failed"
          slackSend(slackParams("Build failed: ${env.JOB_NAME} @ ${env.BUILD_NUMBER} (${BUILD_USER})", "danger"))
        }
      }
    }
  }
}
