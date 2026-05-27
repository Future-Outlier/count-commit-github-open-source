import os
import sys
import requests

def upload_and_embed_image(token, image_path):
    if not os.path.exists(image_path):
        print(f"❌ 錯誤：找不到圖片檔案：{image_path}")
        return

    file_name = os.path.basename(image_path)
    base_url = "https://cwiki.apache.org/confluence/rest/api"
    space_key = "COMDEV"
    title = "ALC Taipei"

    headers = {
        "Authorization": f"Bearer {token}",
        "X-Atlassian-Token": "nocheck"
    }

    # -------------------------------------------------------------
    # 步驟 1：自動查詢頁面 ID
    # -------------------------------------------------------------
    print(f"🔍 正在尋找 '{title}' 頁面的 ID...")
    search_url = f"{base_url}/content"
    params = {"spaceKey": space_key, "title": title}

    try:
        res = requests.get(search_url, headers=headers, params=params)
        res.raise_for_status()
        results = res.json().get("results", [])
        if not results:
            print("❌ 錯誤：找不到該頁面。")
            return
        page_id = results[0]["id"]
        print(f"✅ 成功獲取頁面 ID: {page_id}")
    except Exception as e:
        print(f"❌ 查詢頁面 ID 失敗: {e}")
        return

    # -------------------------------------------------------------
    # 步驟 2：上傳圖片至附件
    # -------------------------------------------------------------
    print(f"📤 正在將 '{file_name}' 上傳至 Confluence 附件庫...")
    upload_url = f"{base_url}/content/{page_id}/child/attachment"
    content_type = "image/jpeg" if file_name.lower().endswith((".jpg", ".jpeg")) else "image/png"

    try:
        with open(image_path, "rb") as f:
            files = {"file": (file_name, f, content_type)}
            upload_res = requests.post(upload_url, headers=headers, files=files)

            # 💡 修正處：同時相容 200 成功、409 重複、以及 Apache 特有的 400 同名錯誤訊息
            if upload_res.status_code == 200:
                print("✅ 圖片上傳附件庫成功！")
            elif upload_res.status_code == 409 or (upload_res.status_code == 400 and "same file name" in upload_res.text):
                print("⚠️ 提示：附件庫已存在同名檔案，將直接嘗試在頁面中嵌入該檔名。")
            else:
                print(f"❌ 附件上傳失敗: {upload_res.text}")
                return
    except Exception as e:
        print(f"❌ 上傳附件發生錯誤: {e}")
        return

    # -------------------------------------------------------------
    # 步驟 3：獲取當前頁面內文與版本號
    # -------------------------------------------------------------
    print("🔄 正在讀取目前網頁的最新版面結構...")
    content_url = f"{base_url}/content/{page_id}"
    content_params = {"expand": "body.storage,version"}

    try:
        content_res = requests.get(content_url, headers=headers, params=content_params)
        content_res.raise_for_status()
        page_data = content_res.json()

        current_version = page_data["version"]["number"]
        current_body = page_data["body"]["storage"]["value"]
        print(f"ℹ️ 當前網頁版本為: V{current_version}")
    except Exception as e:
        print(f"❌ 讀取網頁架構失敗: {e}")
        return

    # -------------------------------------------------------------
    # 步驟 4：組合新內文並更新網頁 (將圖片標籤附加在最尾端)
    # -------------------------------------------------------------
    print("📝 正在編輯網頁原始碼，插入圖片標籤...")

    image_macro = f'<p style="text-align: center;"><ac:image ac:align="center"><ri:attachment ri:filename="{file_name}" /></ac:image></p>'
    new_body = current_body + image_macro

    update_payload = {
        "id": page_id,
        "type": "page",
        "title": title,
        "space": {"key": space_key},
        "body": {
            "storage": {
                "value": new_body,
                "representation": "storage"
            }
        },
        "version": {
            "number": current_version + 1
        }
    }

    print(f"🚀 正在將網頁更新至版本 V{current_version + 1}...")
    try:
        put_res = requests.put(content_url, headers=headers, json=update_payload)
        if put_res.status_code == 200:
            print(f"🎉 成功！圖片已成功貼到網頁內文中。請重新整理瀏覽器查看網頁！")
            print(f"🔗 網頁網址: https://cwiki.apache.org/confluence/x/YYoUFg")
        else:
            print(f"❌ 更新網頁內文失敗，錯誤碼: {put_res.status_code}")
            print(put_res.text)
    except Exception as e:
        print(f"❌ 發送更新請求時發生錯誤: {e}")

if __name__ == "__main__":
    if len(sys.argv) == 3:
        user_token = sys.argv[1]
        user_image_path = sys.argv[2]
        upload_and_embed_image(user_token, user_image_path)
    else:
        print("請帶入參數執行：python upload_to_confluence.py <TOKEN> <圖片路徑>")