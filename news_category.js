import dotenv from "dotenv";
import moment from "moment";
import { MongoClient } from "mongodb";
import mysql from "mysql2/promise";

// Load biến môi trường từ .env
dotenv.config();

// Đọc cấu hình từ .env
const {
    MYSQL_HOST,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE_NEWS,
    MONGO_URL,
    MONGO_DATABASE_ADMIN,
    BATCH_SIZE,
    NEW_SITE_ID,
    OLD_SITE_ID,
} = process.env;

// Kết nối MySQL
async function connectMySQL() {
    return mysql.createConnection({
        host: MYSQL_HOST,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE_NEWS,
    });
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `SELECT * FROM ${tableName} WHERE IdSite=${parseInt(OLD_SITE_ID)} AND IsDeleted=0 LIMIT ${parseInt(limit)} OFFSET ${parseInt(offset)}`;
    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function deleteOldUnit(db, level) {
    await db.collection(level).deleteMany({
        siteId: +NEW_SITE_ID,
        oldId: { $ne: null }  // Ensures oldId is not null
    });
}

async function migrateTable(sqlConnection, mongoDb, tableName) {

    await deleteOldUnit(mongoDb, 'category')


    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);

        const mappedNews = rows.map((row) => {
            return {
                title: row.Name,
                description: row.Description,
                createdAt: row.CreatedAt,
                oldId: row.Id,
                siteId: +NEW_SITE_ID,
            }
        });



        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) {
            const defaultVal = {
                title: "Default",
                description: "",
                createdAt: new Date(),
                oldId: -1,
                siteId: +NEW_SITE_ID,
            }

            mappedNews.push(defaultVal)
            await mongoDb.collection('category').insertMany(mappedNews);
            break;
        };

        await mongoDb.collection('category').insertMany(mappedNews);

        offset += +BATCH_SIZE;
    }

    console.log(`🏁 Hoàn tất di chuyển bảng ${tableName}!`);
}

const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDb = mongoClient.db(MONGO_DATABASE_ADMIN);

    try {
        await migrateTable(sqlConnection, mongoDb, 'PostCategories');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();