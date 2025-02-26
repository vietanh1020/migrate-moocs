import mysql from "mysql2/promise";
import { MongoClient, ObjectId } from "mongodb";
import dotenv from "dotenv";
import moment from "moment";

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

async function deleteOldUnit(db, level) {
    await db.collection(level).deleteMany({
        siteId: +NEW_SITE_ID,
    });
}

async function InsertCateDefault(mongoDb) {
    const collection = mongoDb.collection('category'); // Replace with your actual collection name



    const defaultCategory = {
        _id: new ObjectId(),
        title: "Default",
        description: "description",
        siteId: +NEW_SITE_ID,
        domainSite: null,
        createdAt: moment().unix() // Convert from Unix timestamp to Date
    };

    await collection.insertOne(defaultCategory);

    return defaultCategory;

}
// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `SELECT * FROM ${tableName} WHERE IdSite=${parseInt(OLD_SITE_ID)} LIMIT ${parseInt(limit)} OFFSET ${parseInt(offset)}`;
    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function migrateTable(sqlConnection, mongoDb, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    await deleteOldUnit(mongoDb, 'category')
    await deleteOldUnit(mongoDb, 'news')


    const defaultValue = await InsertCateDefault(mongoDb);

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);

        const mappedNews = rows.map((row) => ({
            slug: row.Slug,
            url_img: row.ThumbnailFileUrl ? row.ThumbnailFileUrl.replace("https://cdn4t.mobiedu.vn", "https://media-moocs.mobifone.vn") : "",
            title: row.Title,
            short_description: row.Description,
            description: row.HtmlContent.replaceAll("https://cdn4t.mobiedu.vn", "https://media-moocs.mobifone.vn"),
            status: row.ApproveStatus,
            createdAt: moment(row.CreatedAt).unix(),
            category: {
                _id: defaultValue._id.toString(),
                title: defaultValue.title
            },
            siteId: +NEW_SITE_ID,
            view_count: row.ViewCounter,
            view_fake: 0,
            pin_type: 0,
        }));

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) break;

        await mongoDb.collection('news').insertMany(mappedNews);

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
        await migrateTable(sqlConnection, mongoDb, 'Post');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();