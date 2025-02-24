import mysql from "mysql2/promise";
import { MongoClient } from "mongodb";
import dotenv from "dotenv";

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
    ID_ADMIN_SITE
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
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `
    SELECT * FROM ${tableName} 
    WHERE IdSite = ${parseInt(OLD_SITE_ID)} 
    AND IdParent IS NULL 
    LIMIT ${parseInt(limit)} 
    OFFSET ${parseInt(offset)}
    `;

    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function migrateTable(sqlConnection, mongoDb, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);

        const mappedNews = rows.map((row) => ({
            name: row.Name,
            managerId: ID_ADMIN_SITE,
            createdAt: null,
            totalUser: 0,
            siteId: +NEW_SITE_ID,
            oldId: row.Id,
        }));

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDb.collection('level-1').insertMany(mappedNews);

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