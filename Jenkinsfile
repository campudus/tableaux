@Library('campudus-jenkins-shared-lib') _

IMAGE_NAME = "campudus/grud-backend"
DEPLOY_DIR = 'build/libs'
LEGACY_ARCHIVE_FILENAME="grud-backend-docker.tar.gz"
DOCKER_BASE_IMAGE_TAG = "build-${BUILD_NUMBER}"

SLACK_CHANNEL = "#grud"

// flag deactivate tests for fast redeployment from jenkins frontend
shouldTest = true

pipeline {
  agent any

  environment {
    BUILD_DATE = sh(returnStdout: true, script: 'date \"+%Y-%m-%d %H:%M:%S\"').trim()
    GIT_COMMIT_DATE = sh(returnStdout: true, script: "git show -s --format=%ci").trim()
    CLEAN_GIT_BRANCH = sh(returnStdout: true, script: "echo $GIT_BRANCH | sed 's/[\\.\\/\\_\\#]/-/g'").trim()
    COMPOSE_PROJECT_NAME = "${IMAGE_NAME}-${CLEAN_GIT_BRANCH}"
  }

  triggers {
    pollSCM('H/5 * * * *')
  }

  options {
    timestamps()
    copyArtifactPermission('*');
  }

  stages {
    stage('Cleanup & Build base docker image') {
      steps {
        sh "echo setup/clean up stuff..."
        // remove images older than two weeks
        sh 'docker rmi $(docker images -f "dangling=true" -q) || true'
        sh 'docker rmi -f $(docker images --filter "reference=campudus/grud-backend*" | grep " [weeks|months|years]* ago" | awk \'{print $3}\') 2>/dev/null || echo "No images to cleanup"'

        echo "Environment variables:"
        sh "printenv | sort"
        echo ""
        echo "Groovy/Pipeline variables:"
        script {
          groovyVars = [:] << getBinding().getVariables()
          groovyVars.each  {k,v -> print "$k = $v"}
        }
        
        sh "docker build -t ${IMAGE_NAME}-cacher --target=cacher ."
      }
    }

    stage('Assemble classes & Assemble test classes') {
      steps {
        sh "docker build -t ${IMAGE_NAME}-builder --target=builder ."
      }
    }

    stage('Test') {
      when {
        expression { shouldTest }
      }
      steps {
        script {
          try {
            sh """
              TEST_IMAGE=${IMAGE_NAME}-builder \
              CLEAN_GIT_BRANCH=${CLEAN_GIT_BRANCH}-build-${BUILD_NUMBER} \
              docker-compose -f ./docker-compose.run-tests.yml up \
              --abort-on-container-exit --exit-code-from grud-backend
            """
          } finally {
            sh "docker cp grud-backend-branch-${CLEAN_GIT_BRANCH}-build-${BUILD_NUMBER}:/usr/src/app/build ./build/"
            junit '**/build/test-results/test/TEST-*.xml' //make the junit test results available in any case (success & failure)
          }
        }
      }
    }

    stage('Build docker image') {
      steps {
        sh """
          docker build \
          --label "GIT_COMMIT=${GIT_COMMIT}" \
          --label "GIT_COMMIT_DATE=${GIT_COMMIT_DATE}" \
          --label "BUILD_DATE=${BUILD_DATE}" \
          -t ${IMAGE_NAME}:${DOCKER_BASE_IMAGE_TAG}-${GIT_COMMIT} \
          -t ${IMAGE_NAME}:latest \
          -f Dockerfile \
          --rm --target=prod .
        """
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
          sh "docker push ${IMAGE_NAME}:${DOCKER_BASE_IMAGE_TAG}-${GIT_COMMIT}"
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
          slackOk(channel: SLACK_CHANNEL, message: "Image pushed to docker registry: ${IMAGE_NAME}:${DOCKER_BASE_IMAGE_TAG}-${GIT_COMMIT}")
        }
      }
    }

    failure {
      wrap([$class: 'BuildUser']) {
        script {
          sh "echo failed"
          slackError(channel: SLACK_CHANNEL)
        }
      }
    }
  }
}
