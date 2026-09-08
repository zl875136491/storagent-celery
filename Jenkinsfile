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
        SOURCE_URL = 'https://github.com/zl875136491/storagent-celery'
        BACKEND_SOURCE_URL = 'https://github.com/zl875136491/storagent.git'
        TEST_IMAGE = 'python:3.12.10-bookworm'
        PYPI_INDEX_URL = 'https://pypi.tuna.tsinghua.edu.cn/simple'
    }

    stages {
        stage('Checkout') {
            steps {
                script {
                    int checkoutAttempt = 0
                    def workerScm = [:]

                    retry(3) {
                        checkoutAttempt++
                        if (checkoutAttempt > 1) {
                            sleep time: (checkoutAttempt == 2 ? 5 : 10), unit: 'SECONDS'
                        }
                        deleteDir()
                        dir('worker/storagent-celery') {
                            workerScm = checkout(scm) ?: [:]
                        }
                        dir('backend/storagent') {
                            checkout([
                                $class: 'GitSCM',
                                branches: [[name: '*/master']],
                                extensions: [
                                    [$class: 'CloneOption', noTags: true, shallow: false, timeout: 20]
                                ],
                                userRemoteConfigs: [[
                                    credentialsId: 'github_storagent_auth',
                                    url: env.BACKEND_SOURCE_URL
                                ]]
                            ])
                        }
                    }

                    env.GIT_COMMIT = workerScm.GIT_COMMIT ?: sh(
                        script: 'git -C worker/storagent-celery rev-parse HEAD',
                        returnStdout: true
                    ).trim()
                    env.BACKEND_GIT_COMMIT = sh(
                        script: 'git -C backend/storagent rev-parse HEAD',
                        returnStdout: true
                    ).trim()

                    if (!(env.GIT_COMMIT ==~ /[0-9a-fA-F]{40}/)) {
                        error("Unable to determine a full Worker Git commit SHA: '${env.GIT_COMMIT}'.")
                    }
                    if (!(env.BACKEND_GIT_COMMIT ==~ /[0-9a-fA-F]{40}/)) {
                        error("Unable to determine a full Backend Git commit SHA: '${env.BACKEND_GIT_COMMIT}'.")
                    }
                    if (!fileExists('backend/storagent/runtimes/mc')) {
                        error('Backend checkout is missing runtimes/mc; Worker image cannot be built.')
                    }

                    env.IS_RELEASE_BRANCH = (env.BRANCH_NAME == 'master' || env.BRANCH_NAME == 'main') ? 'true' : 'false'
                    if (env.IS_RELEASE_BRANCH == 'true') {
                        env.IMAGE_TAG = "sha-${env.GIT_COMMIT.take(12).toLowerCase()}"
                        env.IMAGE_REF = "${env.IMAGE_REPOSITORY}:${env.IMAGE_TAG}"
                        currentBuild.displayName = env.IMAGE_TAG
                        currentBuild.description = "backend=${env.BACKEND_GIT_COMMIT.take(12)}"
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
                      --env PYTHONDONTWRITEBYTECODE=1 \
                      --env PIP_DISABLE_PIP_VERSION_CHECK=1 --env "PIP_INDEX_URL=$PYPI_INDEX_URL" \
                      "$TEST_IMAGE" bash -c '
                        set -Eeuo pipefail
                        python -m pip install --no-cache-dir -r worker/storagent-celery/requirements.txt
                        python -m pip install --no-cache-dir -r backend/storagent/requirements.txt
                        mkdir -p /tmp/syntax-check
                        cp -a worker/storagent-celery backend/storagent /tmp/syntax-check/
                        python -m compileall -q /tmp/syntax-check/storagent-celery /tmp/syntax-check/storagent/src
                        CELERY_BROKER_URL=mongodb://localhost/storagent_celery \
                        PYTHONPATH=/workspace/worker/storagent-celery:/workspace/backend/storagent \
                        python -c \
                          "from celery_app import app; assert app.conf.task_track_started; assert app.conf.broker_transport_options['messages_collection'] == 'celery.messages'"
                      '
                '''
            }
        }

        stage('Build Docker Image') {
            when { expression { env.IS_RELEASE_BRANCH == 'true' } }
            steps {
                sh '''#!/usr/bin/env bash
                    set -Eeuo pipefail
                    docker build --provenance=false --platform linux/amd64 \
                      --file worker/storagent-celery/Dockerfile \
                      --build-arg "VCS_REF=$GIT_COMMIT" --build-arg "IMAGE_VERSION=$IMAGE_TAG" \
                      --build-arg "SOURCE_URL=$SOURCE_URL" --tag "$IMAGE_REF" .
                    test "$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$IMAGE_REF")" = "$GIT_COMMIT"
                    test "$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.component" }}' "$IMAGE_REF")" = 'celery-worker'
                '''
            }
        }

        stage('Push Docker Image') {
            when { expression { env.IS_RELEASE_BRANCH == 'true' } }
            steps {
                withCredentials([usernamePassword(credentialsId: 'infra_harbor_auth', usernameVariable: 'INFRA_HARBOR_USR', passwordVariable: 'INFRA_HARBOR_PSW')]) {
                    sh '''#!/usr/bin/env bash
                        set -Eeuo pipefail
                        set +x

                        DOCKER_CONFIG="$(mktemp -d "${WORKSPACE_TMP:-/tmp}/storagent-docker-config.XXXXXX")"
                        export DOCKER_CONFIG
                        PUSH_LOG="$(mktemp "${WORKSPACE_TMP:-/tmp}/storagent-docker-push.XXXXXX")"

                        cleanup_registry_session() {
                            docker logout "$HARBOR" >/dev/null 2>&1 || true
                            rm -rf "$DOCKER_CONFIG"
                            rm -f "$PUSH_LOG"
                        }
                        trap cleanup_registry_session EXIT HUP INT TERM

                        printf '%s' "$INFRA_HARBOR_PSW" | docker login "$HARBOR" --username "$INFRA_HARBOR_USR" --password-stdin >/dev/null
                        unset INFRA_HARBOR_USR INFRA_HARBOR_PSW

                        docker push "$IMAGE_REF" 2>&1 | tee "$PUSH_LOG"
                        IMAGE_DIGEST="$(awk '/digest: sha256:/{for (i = 1; i <= NF; i++) if ($i ~ /^sha256:/) {print $i; exit}}' "$PUSH_LOG")"

                        if ! printf '%s\n' "$IMAGE_DIGEST" | grep -Eq '^sha256:[0-9a-f]{64}$'; then
                            echo 'Docker push completed without a valid registry digest.' >&2
                            exit 1
                        fi

                        printf '%s\n' \
                            'component=celery-worker' \
                            "repository=$IMAGE_REPOSITORY" \
                            "tag=$IMAGE_TAG" \
                            "digest=$IMAGE_DIGEST" \
                            "reference=$IMAGE_REPOSITORY@$IMAGE_DIGEST" \
                            "gitCommit=$GIT_COMMIT" \
                            "backendGitCommit=$BACKEND_GIT_COMMIT" \
                            "buildNumber=$BUILD_NUMBER" \
                            "buildUrl=${BUILD_URL:-}" \
                            > celery-image.properties
                    '''
                }

                archiveArtifacts(
                    artifacts: 'celery-image.properties',
                    fingerprint: true,
                    onlyIfSuccessful: true
                )
            }
        }
    }

    post {
        always {
            sh '''#!/usr/bin/env bash
                set +e
                if [ -n "${IMAGE_REF:-}" ]; then
                    docker image rm "$IMAGE_REF" >/dev/null 2>&1 || true
                fi
            '''
            deleteDir()
        }
        success {
            script {
                if (env.IS_RELEASE_BRANCH == 'true') {
                    echo "Worker image published as ${env.IMAGE_REF} (backend ${env.BACKEND_GIT_COMMIT})."
                } else {
                    echo "Worker tests passed for ${env.BRANCH_NAME ?: 'an unclassified branch'}; image publishing was skipped."
                }
            }
        }
        failure {
            echo 'Worker image pipeline failed.'
        }
    }
}
