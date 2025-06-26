import pymongo
from pymongo import UpdateOne
from tqdm import tqdm

# === 配置区 ===
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'Unit_Info'  # 已根据用户要求修改
COL_UNIT = 'xfaqpc_jzdwxx'
COL_BUILDING = 'xfaqpc_jzxx'
BATCH_SIZE = 1000


def main():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    unit_col = db[COL_UNIT]
    building_col = db[COL_BUILDING]

    total = unit_col.count_documents({})
    print(f"共需处理 {total} 条单位数据...")

    cursor = unit_col.find({}, {'_id': 1, 'BUILDING_ID': 1, 'ADDRESS': 1})
    batch = []
    n = 0
    for doc in tqdm(cursor, total=total):
        building_id = doc.get('BUILDING_ID')
        if not building_id:
            continue
        # 跳过已存在ADDRESS的
        if doc.get('ADDRESS'):
            continue
        # 查找对应的ADDRESS（用ID字段关联）
        building = building_col.find_one({'ID': building_id}, {'ADDRESS': 1})
        address = building.get('ADDRESS') if building else None
        if not address:
            continue
        batch.append(UpdateOne({'_id': doc['_id']}, {'$set': {'ADDRESS': address}}))
        if len(batch) >= BATCH_SIZE:
            unit_col.bulk_write(batch, ordered=False)
            n += len(batch)
            print(f"已写入 {n} 条...")
            batch = []
    if batch:
        unit_col.bulk_write(batch, ordered=False)
        n += len(batch)
        print(f"已写入 {n} 条（全部完成）")
    print("✅ ADDRESS字段批量补充完成！")
    client.close()

if __name__ == '__main__':
    main() 