pipeline {
    agent any

    parameters {
        choice(
            name: 'DEPLOY_ACTION',
            choices: ['deploy', 'none', 'test_only'],
            description: 'deploy - развернуть всё через compose, test_only - только тесты, none - только сборка'
        )
        string(
            name: 'SCENARIO_FILE',
            defaultValue: 'scenario.json',
            description: 'Файл со сценарием функционального тестирования'
        )
    }

    environment {
        API_IMAGE_NAME        = "4ddocker/lab4-api:${env.BUILD_NUMBER}"
        API_IMAGE_LATEST      = '4ddocker/lab4-api:latest'
        CONSUMER_IMAGE_NAME   = "4ddocker/lab4-consumer:${env.BUILD_NUMBER}"
        CONSUMER_IMAGE_LATEST = '4ddocker/lab4-consumer:latest'
        LOCAL_DATA_PATH       = 'C:\\DopEdu\\ML_ITMO\\DevOpsLab\\Lab4'
        VAULT_PASSWORD        = credentials('vault-password')
        DOCKER_HUB_USER       = '4ddocker'
        DOCKER_HUB_PASS       = credentials('docker')
        DOCKER_HUB_CRED       = 'docker'
    }

    stages {
        stage('Checkout') {
            steps {
                echo '📦 Клонирование репозитория из GitHub...'
                checkout scm
                echo '✅ Код успешно получен'
            }
        }

        stage('Copy Large Files') {
            steps {
                echo '📁 Копирование больших файлов из локальной папки...'
                bat """
            if not exist "data" mkdir data
            if exist "${LOCAL_DATA_PATH}\\data\\*.csv" copy "${LOCAL_DATA_PATH}\\data\\*.csv" data\\
            if not exist "models" mkdir models
            if exist "${LOCAL_DATA_PATH}\\models\\*.pkl" copy "${LOCAL_DATA_PATH}\\models\\*.pkl" models\\
            if exist "${LOCAL_DATA_PATH}\\.env" copy "${LOCAL_DATA_PATH}\\.env" .env
            if not exist "vault" mkdir vault
            if exist "${LOCAL_DATA_PATH}\\vault\\secrets.enc" copy "${LOCAL_DATA_PATH}\\vault\\secrets.enc" vault\\
            echo === .env содержимое ===
            type .env
        """
                echo '✅ Большие файлы скопированы'
            }
        }

        stage('Build Docker Images') {
            steps {
                echo '🏗️ Сборка Docker образов (API + Consumer)...'
                bat "docker build -t ${API_IMAGE_NAME} -f Dockerfile ."
                bat "docker tag ${API_IMAGE_NAME} ${API_IMAGE_LATEST}"
                bat "docker build -t ${CONSUMER_IMAGE_NAME} -f consumer/Dockerfile ."
                bat "docker tag ${CONSUMER_IMAGE_NAME} ${CONSUMER_IMAGE_LATEST}"
                echo '✅ Образы собраны'
            }
        }

        stage('Push to Docker Hub') {
            when { expression { params.DEPLOY_ACTION == 'deploy' } }
            steps {
                script {
                    docker.withRegistry('', DOCKER_HUB_CRED) {
                        docker.image("4ddocker/lab4-api:${env.BUILD_NUMBER}").push()
                        docker.image('4ddocker/lab4-api:latest').push()
                        docker.image("4ddocker/lab4-consumer:${env.BUILD_NUMBER}").push()
                        docker.image('4ddocker/lab4-consumer:latest').push()
                    }
                }
                echo '✅ Образы опубликованы на Docker Hub'
            }
        }

        stage('Deploy with docker-compose') {
            when { expression { params.DEPLOY_ACTION == 'deploy' || params.DEPLOY_ACTION == 'test_only' } }
            steps {
                echo '🛑 Остановка старых контейнеров...'
                bat '''
            docker stop graduate-analytics graduate-consumer graduate-kafka graduate-zookeeper graduate-postgres 2>nul || exit 0
            docker rm graduate-analytics graduate-consumer graduate-kafka graduate-zookeeper graduate-postgres 2>nul || exit 0
        '''

                echo '🚀 Развёртывание через docker-compose...'
                bat 'docker-compose down || true'
                bat 'docker-compose up -d --build'

                echo '⏳ Ожидание готовности API...'
                powershell '''
            $maxWait = 180
            $waited = 0
            while ($waited -lt $maxWait) {
                Start-Sleep -Seconds 5
                $waited += 5
                $status = docker inspect graduate-analytics --format "{{.State.Health.Status}}" 2>$null
                if ($status -eq "healthy") {
                    Write-Host "✅ API healthy за $waited сек"
                    exit 0
                }
                Write-Host "⏳ Ожидание... ($waited сек), статус: $status"
            }
            Write-Host "❌ API не стал healthy за $maxWait сек"
            exit 1
        '''
            }
        }
        stage('Functional Test with Kafka') {
            when { expression { params.DEPLOY_ACTION == 'deploy' || params.DEPLOY_ACTION == 'test_only' } }
            steps {
                echo '🧪 Функциональный тест с проверкой Kafka...'
                powershell '''
            # 1. Токен
            $tokenResp = Invoke-RestMethod -Uri "http://localhost:8000/token" -Method Get
            $token = $tokenResp.access_token
            if (-not $token) { Write-Host "❌ Не удалось получить токен"; exit 1 }
            Write-Host "✅ Токен получен"

            # 2. Predict через Invoke-RestMethod
            $headers = @{
                "Authorization" = "Bearer $token"
                "Content-Type" = "application/json"
            }
            $body = @{ features = @(5.1, 3.5, 1.4, 0.2) } | ConvertTo-Json
            Write-Host "Body: $body"

            try {
                $resp = Invoke-RestMethod -Uri "http://localhost:8000/predict" -Method Post -Headers $headers -Body $body
            } catch {
                Write-Host "❌ Ошибка запроса: $_"
                exit 1
            }

            if (-not $resp.prediction) { Write-Host "❌ Predict не вернул prediction"; exit 1 }
            Write-Host "✅ Prediction: $($resp.prediction) $($resp.class_name)"

            # 3. Ждём Consumer
            Start-Sleep -Seconds 5

            # 4. Проверяем kafka_messages
            $dbCheck = docker exec graduate-postgres psql -U ml_user -d ml_models -t -c "SELECT COUNT(*) FROM kafka_messages;"
            $count = [int]$dbCheck.Trim()
            if ($count -lt 1) { Write-Host "❌ kafka_messages пуста — Consumer не записал"; exit 1 }
            Write-Host "✅ В kafka_messages $count записей — Consumer работает"
        '''
                echo '✅ Функциональный тест пройден'
            }
        }
    }

    post {
        always {
            script {
                echo '=== Логи контейнеров (tail) ==='
                bat 'docker-compose logs --tail 30 || true'
            }
        }
        success {
            echo '🎉 Pipeline успешно выполнен!'
        }
        failure {
            echo '❌ Pipeline завершился с ошибкой. Проверьте логи выше.'
        }
    }
}
