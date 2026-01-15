# import os
# import requests
# import yaml
# import json
# from bs4 import BeautifulSoup
# from urllib.parse import urljoin
# from concurrent.futures import ThreadPoolExecutor
# import urllib3

# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# CACHE_FILE = "download_cache.json"
# MAX_WORKERS = 1 

# def load_config(config_file="config.yaml"):
#     with open(config_file, 'r', encoding='utf-8') as f:
#         return yaml.safe_load(f)

# def load_cache():
#     if os.path.exists(CACHE_FILE):
#         try:
#             with open(CACHE_FILE, 'r') as f:
#                 return set(json.load(f))
#         except: return set()
#     return set()

# def save_cache(cache_set):
#     with open(CACHE_FILE, 'w') as f:
#         json.dump(list(cache_set), f, indent=4)

# def get_rpm_links(url, keywords):
#     """Kiểm tra nếu tên file chứa TẤT CẢ các keywords"""
#     try:
#         response = requests.get(url, verify=False, timeout=20)
#         response.raise_for_status()
#         soup = BeautifulSoup(response.text, 'html.parser')
#         links = soup.find_all('a', href=True)
        
#         matched_links = []
#         for link in links:
#             href = link['href']
#             # Chỉ lấy file .rpm và chứa tất cả từ khóa (không phân biệt hoa thường)
#             if href.lower().endswith('.rpm'):
#                 if all(kw.lower() in href.lower() for kw in keywords):
#                     matched_links.append(urljoin(url, href))
        
#         return matched_links
#     except Exception as e:
#         print(f"   [!] Lỗi quét {url}: {e}")
#         return []

# def download_worker(url, target_dir, cache_set):
#     file_name = url.split('/')[-1]
#     target_path = os.path.join(target_dir, file_name)

#     # Nếu đã tải rồi hoặc có trong cache thì bỏ qua
#     if url in cache_set or os.path.exists(target_path):
#         return None

#     try:
#         with requests.get(url, stream=True, verify=False, timeout=60) as r:
#             r.raise_for_status()
#             with open(target_path, 'wb') as f:
#                 for chunk in r.iter_content(chunk_size=131072): # 128KB chunk cho nhanh
#                     f.write(chunk)
#         return url
#     except Exception as e:
#         print(f"   [!] Lỗi tải {file_name}: {e}")
#         return None

# def main():
#     config = load_config()
#     base_dir = config['settings']['download_dir']
#     cache = load_cache()
#     all_tasks = []

#     print("--- Bước 1: Quét danh sách file ---")
#     for repo in config['repositories']:
#         print(f"Đang quét: {repo['title']}...")
#         links = get_rpm_links(repo['start_url'], repo['file_patterns'])
        
#         target_folder = os.path.join(base_dir, repo['download_folder'])
#         os.makedirs(target_folder, exist_ok=True)
        
#         for link in links:
#             all_tasks.append((link, target_folder))

#     if not all_tasks:
#         print("[-] Không tìm thấy file nào! Vui lòng kiểm tra lại URL hoặc Keywords.")
#         return

#     print(f"\n--- Bước 2: Tải đa luồng ({len(all_tasks)} file) ---")
#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = [executor.submit(download_worker, url, folder, cache) for url, folder in all_tasks]
        
#         for i, f in enumerate(futures):
#             res = f.result()
#             if res:
#                 cache.add(res)
#                 # Lưu cache mỗi 5 file để đảm bảo an toàn dữ liệu
#                 if i % 5 == 0: save_cache(cache)
#                 print(f"   [OK] {res.split('/')[-1]}")

#     save_cache(cache)
#     print("\n--- Hoàn thành! ---")

# if __name__ == "__main__":
#     main()


# import os
# import requests
# import yaml
# import json
# import time
# import random
# from bs4 import BeautifulSoup
# from urllib.parse import urljoin
# from concurrent.futures import ThreadPoolExecutor
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# CACHE_FILE = "download_cache.json"
# MAX_WORKERS = 1  # Giảm xuống 1 để tránh bị chặn

# # Danh sách mirror tốt (thêm https nếu hỗ trợ, ưu tiên gần VN)
# MIRRORS = [
#     "https://mirror.nju.edu.cn/opensuse/",
#     "https://mirrors.tuna.tsinghua.edu.cn/opensuse/",
#     "http://ftp.gwdg.de/pub/linux/opensuse/",
#     "https://mirror.csclub.uwaterloo.ca/opensuse/",
#     "https://mirror.23media.de/opensuse/",
#     "https://download.opensuse.org/"  # fallback chính
# ]

# def create_session():
#     session = requests.Session()
#     session.headers.update({
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
#         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
#         'Accept-Language': 'en-US,en;q=0.9',
#         'Accept-Encoding': 'gzip, deflate, br',
#         'Connection': 'keep-alive',
#         'Upgrade-Insecure-Requests': '1',
#         'Sec-Fetch-Dest': 'document',
#         'Sec-Fetch-Mode': 'navigate',
#         'Sec-Fetch-Site': 'none',
#         'Sec-Fetch-User': '?1',
#     })
#     retries = Retry(
#         total=10,
#         backoff_factor=3,
#         status_forcelist=[429, 500, 502, 503, 504, 403],  # Thêm 429 và 403
#         allowed_methods=["HEAD", "GET", "OPTIONS"]
#     )
#     session.mount('http://', HTTPAdapter(max_retries=retries))
#     session.mount('https://', HTTPAdapter(max_retries=retries))
#     return session

# def get_random_mirror():
#     return random.choice(MIRRORS)

# def adjust_url_to_mirror(original_url):
#     # original_url ví dụ: https://download.opensuse.org/distribution/leap/15.6/repo/oss/x86_64/
#     # Chuyển sang mirror tương ứng
#     mirror = get_random_mirror()
#     # Thay phần domain chính
#     if 'download.opensuse.org' in original_url:
#         path = original_url.split('download.opensuse.org', 1)[-1]
#         return urljoin(mirror, path.lstrip('/'))
#     return original_url  # Nếu đã là mirror thì giữ nguyên

# def get_rpm_links(session, url, keywords, exclude_keywords):
#     try:
#         mirror_url = adjust_url_to_mirror(url)
#         print(f"  → Quét mirror: {mirror_url}")
#         response = session.get(mirror_url, verify=False, timeout=120)  # Tăng timeout
#         response.raise_for_status()
#         soup = BeautifulSoup(response.text, 'html.parser')
#         links = soup.find_all('a', href=True)
        
#         matched_links = []
#         for link in links:
#             href = link['href']
#             if href.lower().endswith('.rpm'):
#                 name_lower = href.lower()
                
#                 match_all = all(kw.lower() in name_lower for kw in keywords)
                
#                 match_none = True
#                 if exclude_keywords:
#                     if any(ex.lower() in name_lower for ex in exclude_keywords):
#                         match_none = False
                
#                 if match_all and match_none:
#                     full_url = urljoin(mirror_url, href)
#                     matched_links.append(full_url)
        
#         return matched_links
#     except Exception as e:
#         print(f" [!] Lỗi quét {url} (mirror {mirror_url}): {e}")
#         # Fallback thử mirror khác nếu fail
#         if "download.opensuse.org" not in url:
#             print("  → Thử fallback về original...")
#             return get_rpm_links(session, url, keywords, exclude_keywords)  # recursive 1 lần
#         return []

# def download_worker(session, url, target_dir, cache_set):
#     file_name = url.split('/')[-1]
#     target_path = os.path.join(target_dir, file_name)
#     if url in cache_set or os.path.exists(target_path):
#         print(f"  - Đã có: {file_name}")
#         return None
    
#     try:
#         time.sleep(random.uniform(4, 12))  # Sleep random 4-12s để tránh rate limit
#         print(f"  → Tải: {file_name}")
#         with session.get(url, stream=True, verify=False, timeout=120) as r:
#             r.raise_for_status()
#             with open(target_path, 'wb') as f:
#                 for chunk in r.iter_content(chunk_size=131072):
#                     f.write(chunk)
#         return url
#     except Exception as e:
#         print(f" [!] Thất bại: {file_name} - {e}")
#         return None

# def main():
#     if not os.path.exists("config.yaml"):
#         print("Không tìm thấy file config.yaml")
#         return
#     config = yaml.safe_load(open("config.yaml", 'r', encoding='utf-8'))
#     base_dir = config['settings']['download_dir']
#     cache = set(json.load(open(CACHE_FILE, 'r')) if os.path.exists(CACHE_FILE) else [])
    
#     session = create_session()
#     all_tasks = []
#     print("--- BƯỚC 1: QUÉT LINK (với mirror random) ---")
#     for repo in config['repositories']:
#         print(f"Đang quét: {repo['title']}...")
#         keywords = repo.get('file_patterns', [])
#         exclude = repo.get('file_not_patterns', [])
        
#         links = get_rpm_links(session, repo['start_url'], keywords, exclude)
        
#         if links:
#             print(f" => Tìm thấy {len(links)} file phù hợp.")
#             target_folder = os.path.join(base_dir, repo['download_folder'])
#             os.makedirs(target_folder, exist_ok=True)
#             for l in links:
#                 all_tasks.append((l, target_folder))
#         else:
#             print(" => Không tìm thấy file.")
    
#     if not all_tasks:
#         print("\n[!] Kết thúc: Không có file nào khớp với cấu hình.")
#         return
    
#     print(f"\n--- BƯỚC 2: TẢI FILE ({len(all_tasks)} file, tốc độ chậm để tránh block) ---")
#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = [executor.submit(download_worker, session, url, folder, cache) for url, folder in all_tasks]
#         for i, f in enumerate(futures):
#             res = f.result()
#             if res:
#                 cache.add(res)
#                 with open(CACHE_FILE, 'w') as cf:
#                     json.dump(list(cache), cf, indent=4)
#                 print(f" [{i+1}/{len(all_tasks)}] Xong: {res.split('/')[-1]}")
    
#     print("\n--- HOÀN THÀNH TẤT CẢ ---")

# if __name__ == "__main__":
#     main()

import os
import requests
import yaml
import json
import time
import random
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CACHE_FILE = "download_cache.json"
MAX_WORKERS = 1

MIRRORS = [
    # Asia - China (thường nhanh nhất từ Hà Nội)
    "https://mirror.nju.edu.cn/opensuse/",
    "https://mirrors.tuna.tsinghua.edu.cn/opensuse/",
    "https://mirrors.ustc.edu.cn/opensuse/",
    "https://mirror.sjtu.edu.cn/opensuse/",
    "https://mirrors.aliyun.com/opensuse/",               # Alibaba Cloud - rất ổn định
    "https://mirrors.bfsu.edu.cn/opensuse/",              # Beijing Foreign Studies Univ
    "https://mirrors.zju.edu.cn/opensuse/",               # Zhejiang Univ

    # Asia - Taiwan
    "https://free.nchc.org.tw/opensuse/",                 # National Center for High-performance Computing
    "https://ftp.yzu.edu.tw/OS/OpenSUSE/",                # Yuan Ze Univ

    # Asia - Japan
    "http://ftp.jaist.ac.jp/pub/Linux/openSUSE/",         # JAIST - thường tốt
    "http://ftp.riken.jp/Linux/opensuse/",                # RIKEN

    # Europe/Global tốt (fallback nếu Asia fail)
    "http://ftp.gwdg.de/pub/linux/opensuse/",
    "https://mirror.csclub.uwaterloo.ca/opensuse/",
    "https://mirror.23media.de/opensuse/",
    "https://ftp.fau.de/opensuse/",                       # Germany FAU
    "https://mirror.koddos.net/opensuse/",                # Netherlands
    "https://mirror.clarkson.edu/opensuse/",              # US - Clarkson
    "https://mirror.fcix.net/opensuse/",                  # US - good peering

    # Official fallback
    "https://download.opensuse.org/"
]

def create_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })
    retries = Retry(total=8, backoff_factor=3, status_forcelist=[429, 500, 502, 503, 504, 403])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def adjust_url_to_mirror(original_url, mirror):
    if 'download.opensuse.org' in original_url:
        path = original_url.split('download.opensuse.org', 1)[-1].lstrip('/')
        return urljoin(mirror.rstrip('/') + '/', path)
    return original_url

def try_get_rpm_links(session, original_url, keywords, exclude_keywords):
    for mirror in MIRRORS:
        try:
            mirror_url = adjust_url_to_mirror(original_url, mirror)
            print(f"  → Thử mirror: {mirror} → {mirror_url}")
            response = session.get(mirror_url, verify=False, timeout=120)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a', href=True)
            
            matched_links = []
            for link in links:
                href = link['href']
                if href.lower().endswith('.rpm'):
                    name_lower = href.lower()
                    match_all = all(kw.lower() in name_lower for kw in keywords)
                    match_none = not any(ex.lower() in name_lower for ex in exclude_keywords) if exclude_keywords else True
                    if match_all and match_none:
                        full_url = urljoin(mirror_url, href)
                        matched_links.append(full_url)
            
            if matched_links:
                print(f"  → Thành công với mirror {mirror}: {len(matched_links)} file")
                return matched_links
            else:
                print(f"  → Mirror {mirror} không có file khớp")
        except Exception as e:
            print(f"  [!] Mirror {mirror} fail: {e}")
            continue  # Thử mirror tiếp theo
    
    print(f"  [!] Tất cả mirror fail cho {original_url}")
    return []

def download_worker(session, url, target_dir, cache_set):
    file_name = url.split('/')[-1]
    target_path = os.path.join(target_dir, file_name)
    if url in cache_set or os.path.exists(target_path):
        print(f"  - Đã có: {file_name}")
        return None
    
    try:
        time.sleep(random.uniform(4, 12))
        print(f"  → Tải: {file_name} từ {url.split('/')[2]}")
        with session.get(url, stream=True, verify=False, timeout=120) as r:
            r.raise_for_status()
            with open(target_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=131072):
                    f.write(chunk)
        return url
    except Exception as e:
        print(f" [!] Thất bại: {file_name} - {e}")
        return None

def main():
    if not os.path.exists("config.yaml"):
        print("Không tìm thấy config.yaml")
        return
    config = yaml.safe_load(open("config.yaml", 'r', encoding='utf-8'))
    base_dir = config['settings']['download_dir']
    cache = set(json.load(open(CACHE_FILE, 'r')) if os.path.exists(CACHE_FILE) else [])
    
    session = create_session()
    all_tasks = []
    print("--- BƯỚC 1: QUÉT LINK (thử tất cả mirror lần lượt) ---")
    for repo in config['repositories']:
        print(f"\nĐang quét: {repo['title']}...")
        keywords = repo.get('file_patterns', [])
        exclude = repo.get('file_not_patterns', [])
        
        links = try_get_rpm_links(session, repo['start_url'], keywords, exclude)
        
        if links:
            print(f" => Tìm thấy {len(links)} file.")
            target_folder = os.path.join(base_dir, repo['download_folder'])
            os.makedirs(target_folder, exist_ok=True)
            for l in links:
                all_tasks.append((l, target_folder))
        else:
            print(" => Không tìm thấy file nào.")
    
    if not all_tasks:
        print("\n[!] Không có file nào để tải.")
        return
    
    print(f"\n--- BƯỚC 2: TẢI {len(all_tasks)} file (chậm để tránh block) ---")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_worker, session, url, folder, cache) for url, folder in all_tasks]
        downloaded_count = 0
        for i, f in enumerate(futures):
            res = f.result()
            if res:
                cache.add(res)
                with open(CACHE_FILE, 'w') as cf:
                    json.dump(list(cache), cf, indent=4)
                downloaded_count += 1
                print(f" [{downloaded_count}/{len(all_tasks)}] Xong: {res.split('/')[-1]}")
    
    print("\n--- HOÀN THÀNH ---")
    print(f"Tổng file tải mới: {downloaded_count}")

if __name__ == "__main__":
    main()