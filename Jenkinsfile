pipeline {
    agent any
    options {
        skipDefaultCheckout(true)
        disableConcurrentBuilds()
        timeout(time: 45, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '20'))
        timestamps()
    }
    environment {
        HARBOR = '10.17.158.118'
        IMAGE_REPOSITORY = '10.17.158.118/storagent/storagent_celery'
        SOURCE_URL = 'https://github.com/zl875136491/storagent'
        TEST_IMAGE = 'python:3.12.10-bookworm'
        PYPI_INDEX_URL = 'https://pypi.tuna.tsinghua.edu.cn/simple'
    }
    stages {
        stage('Checkout') {
            steps {
                script {
                    int checkoutAttempt = 0
                    def scmVars = [:]
                    retry(3) {
                        checkoutAttempt++
                        if (checkoutAttempt > 1) {
                            sleep time: (checkoutAttempt == 2 ? 5 : 10), unit: 'SECONDS'
                        }
                        deleteDir()
                        scmVars = checkout(scm) ?: [:]
                    }
                    env.GIT_COMMIT = scmVars.GIT_COMMIT ?: sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
                    if (!(env.GIT_COMMIT ==~ /[0-9a-fA-F]{40}/)) {
                        error("Unable to determine a full Git commit SHA: '${env.GIT_COMMIT}'.")
                    }
                    if (env.BRANCH_NAME == 'master') {
                        env.IMAGE_TAG = "sha-${env.GIT_COMMIT.take(12).toLowerCase()}"
                        env.IMAGE_REF = "${env.IMAGE_REPOSITORY}:${env.IMAGE_TAG}"
                        currentBuild.displayName = env.IMAGE_TAG
                    }
                }
            }
        }
        stage('Test') {
            steps {
                sh '''#!/usr/bin/env bash
                    set -Eeuo pipefail
                    docker run --rm --platform linux/amd64 \
                      --volume "$WORKSPACE:/workspace:ro" --workdir /workspace \
                      --env PIP_DISABLE_PIP_VERSION_CHECK=1 --env "PIP_INDEX_URL=$PYPI_INDEX_URL" \
                      "$TEST_IMAGE" bash -c '
                        set -Eeuo pipefail
                        python -m pip install --no-cache-dir -r worker/storagent-celery/requirements.txt
                        python -m pip install --no-cache-dir -r backend/storagent/requirements.txt
                        python -m compileall -q backend/storagent/src worker/storagent-celery
                        CELERY_BROKER_URL=mongodb://localhost/storagent_celery \
                        PYTHONPATH=/workspace/worker/storagent-celery:/workspace/backend/storagent \
                        python -c \
                          "from celery_app import app; assert app.conf.task_track_started; assert app.conf.broker_transport_options['messages_collection'] == 'celery.messages'"
                      '
                '''
            }
        }
        stage('Build Docker Image') {
            when { expression { env.BRANCH_NAME == 'master' } }
            steps {
                sh '''#!/usr/bin/env bash
                    set -Eeuo pipefail
                    docker build --provenance=false --platform linux/amd64 \
                      --file worker/storagent-celery/Dockerfile \
                      --build-arg "VCS_REF=$GIT_COMMIT" --build-arg "IMAGE_VERSION=$IMAGE_TAG" \
                      --build-arg "SOURCE_URL=$SOURCE_URL" --tag "$IMAGE_REF" .
                    test "$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$IMAGE_REF")" = "$GIT_COMMIT"
                '''
            }
        }
        stage('Push Docker Image') {
            when { expression { env.BRANCH_NAME == 'master' } }
            steps {
                withCredentials([usernamePassword(credentialsId: 'infra_harbor_auth', usernameVariable: 'INFRA_HARBOR_USR', passwordVariable: 'INFRA_HARBOR_PSW')]) {
                    sh '''#!/usr/bin/env bash
                        set -Eeuo pipefail
                        printf '%s' "$INFRA_HARBOR_PSW" | docker login "$HARBOR" --username "$INFRA_HARBOR_USR" --password-stdin >/dev/null
                        docker push "$IMAGE_REF"
                        docker logout "$HARBOR" >/dev/null 2>&1 || true
                    '''
                }
            }
        }
    }
    post {
        always { sh 'set +e; [ -z "${IMAGE_REF:-}" ] || docker image rm "$IMAGE_REF" >/dev/null 2>&1 || true'; deleteDir() }
    }
}
