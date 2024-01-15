# Ingestão API Centreon

Esse use-case tem como problemática a necessidade de fazer consultas nos endpoints de resources na API do Centreon, software open-source de monitoramento de infraestrutura, e salvar no ADLS gen2 na Azure, a ingestão foi feita usando Databricks.

[https://docs.centreon.com/docs/api/rest-api-v2/](https://docs.centreon.com/docs/api/rest-api-v2/)

O SLA final é de 5 mins de ponta-a-ponta, o que se provou desafiador. Eram cerca de 2000 consultas a cada 5 minutos

As opções consideradas para a solução:

**1)** usar um serviço de mensageria, no caso da Azure o Events Hub, para simular um streaming e receber usando o PySpark no Databricks.

**Prós**: escalável, performático, streaming.

**Contras**: Custo elevado

**2)** Fazer a consulta em batch com trigger de 3 em 3 minutos, consultando todos os endpoints usando armazenando no driver do cluster com Python.

**Prós**: Custo menor

**Contras**: não é escalável, o processamento fica armazenado no driver do cluster o que pode gerar um OOM, tempo pode concorrer com o SLA proposto.

O custo foi algo bem importante para esse projeto, por isso foi decidida a **2a opção.** Segundo nossa análise, o volume de dados não era tão grande para gerar um OOM, e não tinha necessidade de ser tão escalonável, então era possível evitar o uso de Streaming por conta do custo.

Tivemos um problema com a questão da ingestão, que é o use-case demonstrado aqui, a lib ‘requests’, nativa do python para requisições em API Rest, não atendia o SLA pretendido. Somenteo tempo de ingestão dos 2000 URL’s girava em torno de 5 minutos, o que era o nosso tempo alvo de ponta-a-ponta.

Decidimos por usar as libs aiohttp e o asyncio, para manter o custo baixo, sem necessidade de levantar um recurso de mensageria, o que desagradaria o cliente pelo custo. Com essa outra abordagem diminuímos o tempo de ingestão de 5 minutos para cerca de 1 minuto e 20 segundo.

Descrição da Solução:

**1) Consulta no endpoint monitoring (ing_monitoring.ipynb)**

Com o endpoint monitoring eu tenho acesso a todos os hosts que eu preciso consultar, fazendo isso de forma automatizada, sem precisar adicionar manualmente, o que tornaria difícil a manutenção do código. Essa consulta usa a requests, visto que não há necessidade de paralelismo.

**2) Salvar as url’s geradas para consulta numa tabela silver (ing_resources.ipynb)**

A ingestão usa uma função *get_token* que está no caderno **token_transformations.py,** basicamente ela gera um novo token de API caso o anterior esteja expirado.

**3) Fazer a ingestão do endpoint resources**

 **São 2 endpoints resources/hosts e resources/hosts-service, os notebooks estão templatizados. A ingestão foi feita coom base nas url’s geradas na tabela silver usando aiohttp e o asyncio. (exception.ipynb)**

**4)Fluxo de exception**

As task’s de ingestão da resources tem como retorno uma variável que passa para a task de exception, se uma das duas falhar. **parecido com o Xcom do Airflow

Caso um dos jobs falhe, então são apagados os dados daquela execução.
