from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

def limpar_numero(texto):
    """Função para limpar e converter os números do formato brasileiro."""
    try:
        return float(texto.replace('.', '').replace(',', '.'))
    except (ValueError, AttributeError):
        return 0.0

def extrair_dados_da_pagina_atual(driver):
    """Extrai os dados da tabela na página visível no momento."""
    dados_acoes_pagina = []
    try:
        # Espera a tabela estar visível antes de tentar extrair
        tabela = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        # Pula a primeira linha que é o cabeçalho
        linhas = tabela.find_elements(By.TAG_NAME, "tr")[1:]

        for linha in linhas:
            celulas = linha.find_elements(By.TAG_NAME, "td")
            if len(celulas) == 5:
                dados_acao = {
                    "codigo": celulas[0].text.strip(),
                    "acao": celulas[1].text.strip(),
                    "tipo": celulas[2].text.strip(),
                    "qtde_teorica": int(limpar_numero(celulas[3].text.strip())),
                    "participacao_percentual": limpar_numero(celulas[4].text.strip()),
                    "ano": datetime.now().year,
                    "mes": datetime.now().month,
                    "dia": datetime.now().day
                }
                dados_acoes_pagina.append(dados_acao)
    except TimeoutException:
        print("A tabela não foi encontrada na página atual.")
    return dados_acoes_pagina

def raspar_todas_as_paginas_b3():
    """
    Função principal para fazer o scraping de todas as páginas da composição do IBOVESPA.
    """
    url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
    
    print("Iniciando o WebDriver...")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    
    print(f"Acessando a URL: {url}")
    driver.get(url)

    # Espera inicial para a primeira página carregar
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
        )
    except TimeoutException:
        print("Erro: A tabela inicial não carregou. Abortando.")
        driver.quit()
        return None

    todos_os_dados = []
    pagina_atual = 1

    while True:
        print(f"--- Raspando dados da Página {pagina_atual}... ---")
        
        # Extrai os dados da página visível
        dados_da_pagina = extrair_dados_da_pagina_atual(driver)
        if dados_da_pagina:
            todos_os_dados.extend(dados_da_pagina)
            print(f"Encontrados {len(dados_da_pagina)} ativos nesta página.")
        else:
            print("Nenhum dado encontrado na página. Verifique o seletor da tabela.")
            break

        # Lógica para encontrar e clicar no botão "Próxima"
        try:
            # Encontra o elemento <li> que contém o botão "Próxima"
            # A chave é verificar se ele tem a classe 'disabled'
            li_proxima_pagina = driver.find_element(By.CSS_SELECTOR, ".pagination-next")
            
            # Se a classe 'disabled' estiver presente, chegamos ao fim
            if 'disabled' in li_proxima_pagina.get_attribute('class'):
                print("Chegamos na última página. Finalizando a raspagem.")
                break
            
            # Se não, encontra o link <a> dentro do <li> e clica
            botao_proxima_pagina = li_proxima_pagina.find_element(By.TAG_NAME, "a")
            
            # Antes de clicar, pega uma referência da tabela atual para esperar ela mudar
            primeiro_codigo_antes = driver.find_element(By.CSS_SELECTOR, "tbody tr:first-child td:first-child").text

            botao_proxima_pagina.click()
            print("Clicando em 'Próxima'. Aguardando a próxima página carregar...")

            # Espera a tabela antiga se tornar obsoletos, indicando que a DOM mudou
            WebDriverWait(driver, 15).until(
               lambda driver: driver.find_element(By.CSS_SELECTOR, "tbody tr:first-child td:first-child").text != primeiro_codigo_antes
            )

            pagina_atual += 1

        except NoSuchElementException:
            print("Não foi possível encontrar o botão 'Próxima'. Finalizando a raspagem.")
            break

    driver.quit()

    df = pd.DataFrame(todos_os_dados)
    return df

if __name__ == "__main__":
    df_ibovespa_completo = raspar_todas_as_paginas_b3()
    
    if df_ibovespa_completo is not None and not df_ibovespa_completo.empty:
        print("\nAmostra dos Dados Extraídos (Primeiras 5 linhas)")
        print(df_ibovespa_completo.head())
        
        print("\nAmostra dos Dados Extraídos (Últimas 5 linhas)")
        print(df_ibovespa_completo.tail())

        print(f"\nTotal de ativos raspados: {len(df_ibovespa_completo)}")

        bucket = "s3://henrique-b3/raw/"

        nome_arquivo = "ibovespa"
        df_ibovespa_completo.to_csv(f"{nome_arquivo}.csv", index=False, sep=';', encoding='utf-8-sig')
        df_ibovespa_completo.to_parquet(
            bucket, 
            engine='pyarrow', 
            compression='snappy',
            partition_cols=['ano', 'mes', 'dia'],
            basename_template="dados-{i}.parquet")
        print(f"\nDados salvos com sucesso no arquivo '{nome_arquivo}'")