import dotenv from "dotenv";
import { MongoClient } from "mongodb";
import mysql from "mysql2/promise";

// Load biến môi trường từ .env
dotenv.config();

// Đọc cấu hình từ .env
const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE_COURSE,
    MYSQL_DATABASE_AUTH,
    MONGO_URL,
    MONGO_DATABASE_ADMIN,
    BATCH_SIZE,
    OLD_SITE_ID,
    NEW_SITE_ID,
} = process.env;

// Kết nối MySQL
async function connectMySQL() {
    return mysql.createConnection({
        host: MYSQL_HOST,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE_AUTH,
    });
}

async function deleteOldData(db, table) {
    await db.collection(table).deleteMany({
        siteId: +NEW_SITE_ID,
        oldId: { $ne: null }  // Ensures oldId is not null
    });
}

async function getMaxID(db, table) {
    const result = await db.collection(table)
        .find({})
        .sort({ _id: -1 })
        .limit(1)
        .toArray();

    return result[0]?._id || 0
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `
    SELECT * FROM ${tableName} 
    WHERE IdSite = ${parseInt(OLD_SITE_ID)} 
    AND IsDeleted = 0
    LIMIT ${parseInt(limit)} 
    OFFSET ${parseInt(offset)}
    `;

    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function migrateTable(sqlConnection, mongoDb, tableName) {

    await deleteOldData(mongoDb, 'position')
    const maxId = await getMaxID(mongoDb, 'position')
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);

        const mappedNews = rows.map((row, index) => {
            return {
                _id: maxId + offset + index + 1,
                name: row.Name,
                desc: row.Description,
                createdAt: new Date(row.CreatedAt),
                oldId: row.Id,
                siteId: +NEW_SITE_ID,
            }
        });

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDb.collection('position').insertMany(mappedNews);

        offset += +BATCH_SIZE;
    }

    console.log(`🏁 Hoàn tất di chuyển bảng ${tableName} !`);
}

const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDb = mongoClient.db(MONGO_DATABASE_ADMIN);

    try {
        await migrateTable(sqlConnection, mongoDb, 'JobPositions');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();