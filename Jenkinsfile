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
        echo "Cleanup"
        sh './gradlew clean'
      }
    }

    stage('Assemble test classes') {
      steps {
        echo "Assemble test classes"
        sh './gradlew testClasses'
      }
    }

    stage('Assemble') {
      steps {
        echo "Assemble"
        sh './gradlew assemble'
      }
    }

    stage('Test') {
      steps {
        echo "Test"

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
           slackSend(slackParams("Build successful: ${env.JOB_NAME} @ ${env.BUILD_NUMBER} (${BUILD_USER})", "good"))
         }
       }
     }

     failure {
       wrap([$class: 'BuildUser']) {
         script {
           slackSend(slackParams("Build failed: ${env.JOB_NAME} @ ${env.BUILD_NUMBER} (${BUILD_USER})", "danger"))
         }
       }
     }
   }
}