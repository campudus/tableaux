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
              configFileProvider([configFile(fileId: 'tableaux-backend-build', targetLocation: 'conf-test.json')]) {
                sh './gradlew test'
              }
          } finally {
            junit '**/build/test-results/test/TEST-*.xml' //make the junit test results available in any case (success & failure)
          }
        }
      }
    }

    stage('Archive artifacts') {
      steps {
        archiveArtifacts artifacts: 'build/libs/*-fat.jar', fingerprint: true
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