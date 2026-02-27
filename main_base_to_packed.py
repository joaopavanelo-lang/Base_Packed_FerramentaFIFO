import asyncio
from playwright.async_api import async_playwright
import time
import datetime
import os
import shutil
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials 
import zipfile
import gc
import traceback

DOWNLOAD_DIR = "/tmp/shopee_automation"

# === COLOQUE O ID DA SUA PLANILHA ABAIXO ===
SPREADSHEET_ID = "1TPjzvE8n-NdY2wwoToWYWduhGSID7ATishyvdM0YNRk" 
# ===========================================

def rename_downloaded_file(download_dir, download_path):
    """Renames the downloaded file to include the current hour."""
    try:
        current_hour = datetime.datetime.now().strftime("%H")
        new_file_name = f"TO-Packed{current_hour}.zip"
        new_file_path = os.path.join(download_dir, new_file_name)
        if os.path.exists(new_file_path):
            os.remove(new_file_path)
        shutil.move(download_path, new_file_path)
        print(f"Arquivo salvo como: {new_file_path}")
        return new_file_path
    except Exception as e:
        print(f"Erro ao renomear o arquivo: {e}")
        return None

def unzip_and_process_data(zip_path, extract_to_dir):
    try:
        unzip_folder = os.path.join(extract_to_dir, "extracted_files")
        os.makedirs(unzip_folder, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_folder)
        print(f"Arquivo '{os.path.basename(zip_path)}' descompactado.")

        csv_files = [os.path.join(unzip_folder, f) for f in os.listdir(unzip_folder) if f.lower().endswith('.csv')]
        
        if not csv_files:
            print("Nenhum arquivo CSV encontrado no ZIP.")
            shutil.rmtree(unzip_folder)
            return None

        print(f"Lendo e unificando {len(csv_files)} arquivos CSV...")
        all_dfs = [pd.read_csv(file, encoding='utf-8') for file in csv_files]
        df_final = pd.concat(all_dfs, ignore_index=True)

        # === UNIFICAÇÃO PELA COLUNA A ===
        total_linhas_antes = len(df_final)
        print(f"Linhas antes de unificar: {total_linhas_antes}")
        
        # Pega o nome exato da primeira coluna (Coluna A / Índice 0)
        nome_coluna_a = df_final.columns[0]
        
        # Remove as duplicatas mantendo apenas a primeira ocorrência de cada chave
        df_final = df_final.drop_duplicates(subset=[nome_coluna_a], keep='first')
        
        total_linhas_depois = len(df_final)
        print(f"Processamento concluído. Linhas após unificar pela Coluna A: {total_linhas_depois} (Foram removidas {total_linhas_antes - total_linhas_depois} duplicatas).")
        
        shutil.rmtree(unzip_folder)
        return df_final
        
    except Exception as e:
        print(f"Erro ao processar dados: {e}")
        return None

def update_google_sheet_with_dataframe(df_to_upload):
    """Updates a Google Sheet using native gspread methods and modern auth."""
    if df_to_upload is None or df_to_upload.empty:
        print("Nenhum dado para enviar.")
        return
        
    try:
        print(f"Preparando envio de {len(df_to_upload)} linhas para o Google Sheets...")
        
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        if not os.path.exists("hxh.json"):
            raise FileNotFoundError("O arquivo 'hxh.json' não foi encontrado.")

        creds = Credentials.from_service_account_file("hxh.json", scopes=scope)
        client = gspread.authorize(creds)
        
        print(f"Abrindo planilha pelo ID: {SPREADSHEET_ID}...")
        planilha = client.open_by_key(SPREADSHEET_ID)
        aba = planilha.worksheet("Base")
        
        print("Limpando a aba 'Base'...")
        aba.clear() 
        
        headers = df_to_upload.columns.tolist()
        aba.append_rows([headers], value_input_option='USER_ENTERED')
        
        df_to_upload = df_to_upload.fillna('')
        dados_lista = df_to_upload.values.tolist()
        
        # Mantive os lotes em 15.000 para ser rápido
        chunk_size = 15000 
        total_chunks = (len(dados_lista) // chunk_size) + 1
        
        print(f"Iniciando upload otimizado de {len(dados_lista)} registros em {total_chunks} lotes...")

        for i in range(0, len(dados_lista), chunk_size):
            chunk = dados_lista[i:i + chunk_size]
            aba.append_rows(chunk, value_input_option='USER_ENTERED')
            print(f" -> Lote {i//chunk_size + 1}/{total_chunks} enviado.")
            time.sleep(1)
        
        print("✅ SUCESSO! Dados enviados para o Google Sheets.")

    except Exception as e:
        print("❌ ERRO CRÍTICO NO UPLOAD:")
        print(f"Mensagem de erro: {str(e)}")
        traceback.print_exc()

async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False, 
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--window-size=1920,1080"]
        )
        context = await browser.new_context(accept_downloads=True, viewport={"width": 1920, "height": 1080})
        page = await context.new_page()
        try:
            print("Realizando login...")
            await page.goto("https://spx.shopee.com.br/")
            await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=15000)
            await page.locator('xpath=//*[@placeholder="Ops ID"]').fill('Ops141166')
            await page.locator('xpath=//*[@placeholder="Senha"]').fill('@Shopee123')
            await page.locator('xpath=/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button').click()
            await page.wait_for_timeout(10000)
            
            try:
                if await page.locator('.ssc-dialog-close').is_visible():
                    await page.locator('.ssc-dialog-close').click()
            except:
                pass
            
            print("Navegando...")
            await page.goto("https://spx.shopee.com.br/#/general-to-management")
            await page.wait_for_timeout(8000)
            
            try:
                if await page.locator('.ssc-dialog-wrapper').is_visible():
                     await page.keyboard.press("Escape")
                     await page.wait_for_timeout(1000)
            except:
                pass

            print("Exportando...")
            await page.get_by_role('button', name='Exportar').click(force=True)
            await page.wait_for_timeout(5000)
            await page.locator('xpath=/html[1]/body[1]/span[4]/div[1]/div[1]/div[1]').click(force=True)
            await page.wait_for_timeout(5000)
            await page.get_by_role("treeitem", name="Packed", exact=True).click(force=True)
            await page.wait_for_timeout(5000)
            await page.get_by_role("button", name="Confirmar").click(force=True)
            
            print("Aguardando geração do relatório...")
            await page.wait_for_timeout(60000) 
            
            print("Baixando...")
            async with page.expect_download(timeout=120000) as download_info:
                await page.get_by_role("button", name="Baixar").first.click(force=True)
            
            download = await download_info.value
            download_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            await download.save_as(download_path)
            print(f"Download concluído: {download_path}")

            renamed_zip_path = rename_downloaded_file(DOWNLOAD_DIR, download_path)
            
            if renamed_zip_path:
                final_dataframe = unzip_and_process_data(renamed_zip_path, DOWNLOAD_DIR)
                update_google_sheet_with_dataframe(final_dataframe)
                
                if final_dataframe is not None:
                    del final_dataframe
                    gc.collect()

        except Exception as e:
            print(f"Erro durante a execução do Playwright: {e}")
            traceback.print_exc()
        finally:
            await browser.close()
            if os.path.exists(DOWNLOAD_DIR):
                shutil.rmtree(DOWNLOAD_DIR)
                print("Limpeza concluída.")

if __name__ == "__main__":
    asyncio.run(main())
