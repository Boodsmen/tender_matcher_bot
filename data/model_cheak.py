import os
import glob
import re

# ===================== НАСТРОЙКИ =====================
FOLDER_PATH = r'C:\Users\1\Desktop\my projects\tgbot\equipment-matcher-bot\data\csv'
# =====================================================

def clean_filenames():
    print(f"🚀 ЧИСТКА ИМЕН ФАЙЛОВ...\n   Папка: {FOLDER_PATH}")
    
    # Ищем все xlsx файлы
    files = glob.glob(os.path.join(FOLDER_PATH, "*.xlsx"))
    
    if not files:
        print("❌ Файлов не найдено.")
        return

    count = 0
    for file_path in files:
        directory = os.path.dirname(file_path)
        old_filename = os.path.basename(file_path)
        
        # 1. Удаляем 'config' (независимо от регистра)
        # re.IGNORECASE делает поиск нечувствительным к регистру
        new_filename = re.sub(r'config', '', old_filename, flags=re.IGNORECASE)
        
        # 2. Меняем пробелы на _
        new_filename = new_filename.replace(' ', '_')
        
        # 3. Чистка мусора (чтобы не было __ или _.)
        # Заменяем повторяющиеся подчеркивания на одно
        new_filename = re.sub(r'_+', '_', new_filename)
        
        # Разделяем имя и расширение, чтобы почистить "хвосты"
        name_root, ext = os.path.splitext(new_filename)
        
        # Убираем подчеркивания по краям (например "_MES_10_.xlsx" -> "MES_10.xlsx")
        name_root = name_root.strip('_')
        
        # Собираем обратно
        final_filename = name_root + ext
        
        # Если имя реально изменилось - переименовываем
        if final_filename != old_filename:
            old_path = os.path.join(directory, old_filename)
            new_path = os.path.join(directory, final_filename)
            
            try:
                os.rename(old_path, new_path)
                print(f"   ✅ {old_filename} \n      ---> {final_filename}")
                count += 1
            except FileExistsError:
                print(f"   ⚠️ Нельзя переименовать: {final_filename} уже существует!")
            except PermissionError:
                print(f"   ❌ Ошибка: Закрой файл {old_filename} в Excel!")

    print("\n" + "="*40)
    print(f"🏁 Готово! Переименовано файлов: {count}")
    print("="*40)

if __name__ == "__main__":
    clean_filenames()