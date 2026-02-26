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
import sys

DOWNLOAD_DIR = "/tmp/shopee_automation"

# === ID DA NOVA PLANILHA DESTINO ===
SPREADSHEET_ID = "1TPjzvE8n-NdY2wwoToWYWduhGSID7ATishyvdM0YNRk" 
# ===================================

def rename_downloaded_file(download_dir, download_path):
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
    """Extrai os CSVs do ZIP e retorna o DataFrame bruto (sem filtros)."""
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

        print(f"Lendo {len(csv_files)} arquivos CSV(s)...")
        all_dfs = [pd.read_csv(file, encoding='utf-8') for file in csv_files]
        df_final = pd.concat(all_dfs, ignore_index=True)

        print(f"Leitura concluída. A base de dados tem {len(df_final)} linhas e {len(df_final.columns)} colunas no total.")
        
        shutil.rmtree(unzip_folder)
        return df_final
        
    except Exception as e:
        print(f"Erro ao extrair e ler dados: {e}")
        return None

def update_google_sheet_with_dataframe(df_to_upload):
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
        aba = planilha.worksheet("Packed")
        
        print("Limpando a aba 'Packed'...")
        aba.clear() 
        
        headers = df_to_upload.columns.tolist()
        aba.append_rows([headers], value_input_option='USER_ENTERED')
        
        df_to_upload = df_to_upload.fillna('')
        dados_lista = df_to_upload.values.tolist()
        
        chunk_size = 2000
        total_chunks = (len(dados_lista) // chunk_size) + 1
        
        print(f"Iniciando upload de {len(dados_lista)} registros em {total_chunks} lotes...")

        for i in range(0, len(dados_lista), chunk_size):
            chunk = dados_lista[i:i + chunk_size]
            aba.append_rows(chunk, value_input_option='USER_ENTERED')
            print(f" -> Lote {i//chunk_size + 1}/{total_chunks} enviado.")
            time.sleep(2) 
        
        print("✅ SUCESSO! Dados enviados para o Google Sheets.")
        time.sleep(2)

    except Exception as e:
        print("❌ ERRO CRÍTICO NO UPLOAD:")
        print(f"Mensagem de erro: {str(e)}")
        traceback.print_exc()

async def main():
    shopee_user = os.environ.get("SHOPEE_USER")
    shopee_pass = os.environ.get("SHOPEE_PASS")

    if not shopee_user or not shopee_pass:
        print("❌ ERRO: Variáveis de ambiente SHOPEE_USER ou SHOPEE_PASS não foram encontradas!")
        sys.exit(1)

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False, 
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--window-size=1920,1080"]
        )
        
        # --- FORÇA O NAVEGADOR A SER BRASILEIRO ---
        context = await browser.new_context(
            accept_downloads=True, 
            viewport={"width": 1920, "height": 1080},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo"
        )
        page = await context.new_page()
        
        try:
            # === LOGIN ===
            print("Realizando login...")
            await page.goto("https://spx.shopee.com.br/")
            await page.wait_for_selector('xpath=//*[@placeholder="Ops ID"]', timeout=15000)
            
            await page.locator('xpath=//*[@placeholder="Ops ID"]').fill(shopee_user)
            await page.locator('xpath=//*[@placeholder="Senha"]').fill(shopee_pass)
            await page.locator('xpath=/html/body/div[1]/div/div[2]/div/div/div[1]/div[3]/form/div/div/button').click()
            await page.wait_for_timeout(10000)
            
            try:
                if await page.locator('.ssc-dialog-close').is_visible():
                    await page.locator('.ssc-dialog-close').click()
            except:
                pass
            
            # === NAVEGAÇÃO E EXPORTAÇÃO ===
            print("Navegando...")
            await page.goto("https://spx.shopee.com.br/#/general-to-management", wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)
            
            print("Verificando pop-ups na tela de exportação...")
            try:
                if await page.locator('.ssc-dialog-wrapper').is_visible():
                     await page.keyboard.press("Escape")
                     await page.wait_for_timeout(1000)
            except:
                pass

            print("Aguardando o botão Exportar aparecer na tela...")
            # --- ESPERA INTELIGENTE E MAIS FLEXÍVEL ---
            btn_exportar = page.locator("text='Exportar'").first
            await btn_exportar.wait_for(state="visible", timeout=30000)
            print("Botão encontrado! Clicando em Exportar...")
            
            await btn_exportar.click(force=True)
            await page.wait_for_timeout(5000)
            await page.locator('xpath=/html[1]/body[1]/span[4]/div[1]/div[1]/div[1]').click(force=True)
            await page.wait_for_timeout(5000)
            await page.get_by_role("treeitem", name="Packed", exact=True).click(force=True)
            await page.wait_for_timeout(5000)
            await page.get_by_role("button", name="Confirmar").click(force=True)
            
            print("Aguardando geração do relatório...")
            await page.wait_for_timeout(60000) 
            
            # === DOWNLOAD ===
            print("Baixando...")
            async with page.expect_download(timeout=120000) as download_info:
                await page.get_by_role("button", name="Baixar").first.click(force=True)
            
            download = await download_info.value
            download_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            await download.save_as(download_path)
            print(f"Download concluído: {download_path}")

            # === PROCESSAMENTO E UPLOAD ===
            renamed_zip_path = rename_downloaded_file(DOWNLOAD_DIR, download_path)
            
            if renamed_zip_path:
                final_dataframe = unzip_and_process_data(renamed_zip_path, DOWNLOAD_DIR)
                update_google_sheet_with_dataframe(final_dataframe)
                
                if final_dataframe is not None:
                    del final_dataframe
                    gc.collect()

        except Exception as e:
            print(f"\n❌ Erro durante a execução do Playwright: {e}")
            traceback.print_exc()
            
            # --- TIRA A FOTO EM CASO DE ERRO ---
            print("Salvando screenshot do erro para análise...")
            try:
                await page.screenshot(path="erro_shopee.png", full_page=True)
                print("Screenshot salvo como 'erro_shopee.png'")
            except Exception as screenshot_err:
                print(f"Não foi possível tirar screenshot: {screenshot_err}")
                
            # Interrompe o script informando o erro ao GitHub Actions
            sys.exit(1)
            
        finally:
            await browser.close()
            if os.path.exists(DOWNLOAD_DIR):
                shutil.rmtree(DOWNLOAD_DIR)
                print("Limpeza do diretório concluída.")

if __name__ == "__main__":
    asyncio.run(main())
