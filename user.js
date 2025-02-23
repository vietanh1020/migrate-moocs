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
    MYSQL_DATABASE_AUTH,
    MONGO_URL,
    MONGO_DATABASE_ADMIN,
    BATCH_SIZE,
    NEW_SITE_ID,
    MYSQL_DATABASE_4T,
    OLD_SITE_ID,
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

async function connectTableRole() {
    return mysql.createConnection({
        host: MYSQL_HOST,
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE_4T,
    });
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `SELECT * FROM ${tableName} WHERE IdSite=${OLD_SITE_ID} LIMIT ${parseInt(limit)} OFFSET ${parseInt(offset)}`;
    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}

async function findRole(records) {
    if (records.length == 0) return [];
    const ids = records.map(item => item.Id)
    const query = `SELECT * FROM users WHERE mobiedu_user_id IN (${ids.join(",")})`;
    const [rows] = await sqlConnectionRole.execute(query);
    return rows;
}

async function migrateTable(sqlConnection, mongoDb, tableName) {
    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);

        const roles = await findRole(rows);

        const mappedUsers = rows.map((row) => ({
            accessFailedCount: 0,
            address: "",
            avatar: row.AvatarUrl ? row.AvatarUrl.replace("https://cdn4t.mobiedu.vn", "https://media-moocs.mobifone.vn") : "",
            birthday: new Date(row.Birthday),
            dateLogin: row.LastLoginDate,
            deletedOn: null,
            email: row.Email || "",
            fullname: row.FullName,
            gender: row.IdGender || 0,
            exploreField: 1,
            siteId: +NEW_SITE_ID,
            positionId: "",
            infoManagementLevel: null,
            isLockoutEnabled: row.IsBlocked == 0 ? true : false,
            lockoutEndDate: null,
            passwordHash: row.Pwd,
            phone: row.Phone || "",
            pwd: "Migrate@2025",
            securityStamp: "",
            status: 1,
            timeUpdate: null,
            userName: row.Email,
            mobieduUserId: row.Id,
            listRoles: null,
            listPolicy: null,
            role: roles.find(item => item.mobiedu_user_id == row.Id)?.role == "student" ? "STUDENT" : "ADMIN",
            functionsTree: null,
        }));

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) {
            break;
        }

        await mongoDb.collection('user').insertMany(mappedUsers);

        offset += BATCH_SIZE;
    }

    console.log(`🏁 Hoàn tất di chuyển bảng ${tableName}!`);
}

const sqlConnectionRole = await connectTableRole()
const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDb = mongoClient.db(MONGO_DATABASE_ADMIN);

    try {
        await migrateTable(sqlConnection, mongoDb, 'Users');
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();
