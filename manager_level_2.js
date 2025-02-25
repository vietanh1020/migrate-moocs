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
async function fetchBatch(sqlConnection, tableName, offset, limit, level1OldIds) {
    const query = `
    SELECT * FROM ${tableName}
    WHERE IdSite = ${parseInt(OLD_SITE_ID)}
    AND IdParent IN (${level1OldIds.map(id => parseInt(id)).join(",")})
    LIMIT ${parseInt(limit)} OFFSET ${parseInt(offset)}
    `;

    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function findLevel1(db) {
    const rows = await db.collection("level-1").find().toArray();
    return rows;
}


async function migrateTable(sqlConnection, mongoDb, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    let offset = 0;

    const level1 = await findLevel1(mongoDb)
    const level1OldIds = level1.map(item => item.oldId)


    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE, level1OldIds);

        const mappedNews = rows.map((row) => {
            const level1InDB = level1.find(item => item.oldID === row.IdParent)
            return {
                name: row.Name,
                managerId: ID_ADMIN_SITE,
                createdAt: null,
                totalUser: 0,
                level1: level1InDB?._id,
                level1_name: level1InDB?.name,
                oldId: row.Id,
                siteId: + NEW_SITE_ID
            }
        });

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDb.collection('level-2').insertMany(mappedNews);

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