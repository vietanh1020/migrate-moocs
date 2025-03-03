import mysql from "mysql2/promise";
import { MongoClient } from "mongodb";
import dotenv from "dotenv";
import moment from "moment";

// Load biến môi trường từ .env
dotenv.config();

// Đọc cấu hình từ .env
const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE_LEVEL,
    MONGO_DATABASE_LEVEL,
    MONGO_URL,
    BATCH_SIZE,
    NEW_SITE_ID,
    OLD_SITE_ID,
    ID_ADMIN_SITE,
} = process.env;

// Kết nối MySQL
async function connectMySQL() {
    return mysql.createConnection({
        host: MYSQL_HOST,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE_LEVEL,
    });
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit, level2_OldIds) {
    const query = `
    SELECT * FROM ${tableName}
    WHERE IdSite = ${parseInt(OLD_SITE_ID)}
    AND IsDeleted = 0
    AND IdParent IN (${level2_OldIds.map(id => parseInt(id)).join(",")})
    LIMIT ${parseInt(limit)} OFFSET ${parseInt(offset)}
    `;

    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function findLevel2(db) {
    const rows = await db.collection("level-2").find({
        oldId: { $ne: null },
        siteId: +NEW_SITE_ID
    }).toArray();
    return rows;
}

async function deleteOldUnit(db, level) {
    await db.collection(level).deleteMany({
        siteId: +NEW_SITE_ID,
    });
}

async function migrateTable(sqlConnection, mongoDb, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);
    await deleteOldUnit(mongoDb, "level-3")
    let offset = 0;

    const level2 = await findLevel2(mongoDb)
    const level2_OldIds = level2.map(item => item.oldId)
    if (level2_OldIds.length == 0) return

    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE, level2_OldIds);

        const mappedNews = rows.map((row) => {
            const level2Find = level2.find(item => item.oldId === row.IdParent)
            return {
                name: row.Name,
                managerId: ID_ADMIN_SITE,
                createdAt: new Date(),
                totalUser: 0,
                oldId: row.Id,
                level1: level2Find?.level1.toString(),
                level1_name: level2Find?.level1_name,
                level2: level2Find?._id.toString(),
                level2_name: level2Find?.name,
                siteId: +NEW_SITE_ID
            }
        });

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDb.collection('level-3').insertMany(mappedNews);

        offset += +BATCH_SIZE;
    }
}

const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDb = mongoClient.db(MONGO_DATABASE_LEVEL);

    try {
        await migrateTable(sqlConnection, mongoDb, 'UnitManagements');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();