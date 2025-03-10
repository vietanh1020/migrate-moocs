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
    MYSQL_DATABASE_AUTH,
    MONGO_URL,
    MONGO_DATABASE_LEVEL,
    MONGO_DATABASE_USER,
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
        database: MYSQL_DATABASE_AUTH,
    });
}

function getRoleName(IdType) {
    if (IdType == 2) return "ADMIN"
    if (IdType == 3) return "TEACHER"
    if (IdType == 4) return "STUDENT"
    return ""
}

// Lấy dữ liệu theo từng batch
async function fetchBatch(sqlConnection, tableName, offset, limit) {
    const query = `SELECT * FROM ${tableName} WHERE IdSite=${OLD_SITE_ID} AND IsDeleted=0 LIMIT ${parseInt(limit)} OFFSET ${parseInt(offset)}`;
    const [rows] = await sqlConnection.execute(query);

    console.log(`🟢 Lấy ${rows.length} bản ghi từ ${tableName} (Offset: ${offset})`);
    return rows;
}


async function fetchManagerLevel(sqlConnection, IdUser) {
    const query = `SELECT * FROM UsersInUnitManagements WHERE IdUser=${IdUser}`;
    const [rows] = await sqlConnection.execute(query);

    return rows.map(item => item.IdUnit);
}

async function findManagerLevel(mongoDbLevel, level) {
    const rows = await mongoDbLevel.collection(level).find({ siteId: + NEW_SITE_ID }).toArray();
    return rows;
}

async function deleteOldUsers(db) {
    await db.collection('user').deleteMany({
        siteId: +NEW_SITE_ID,
        mobieduUserId: { $ne: null }  // Ensures oldId is not null
    });
}

async function migrateTable(sqlConnection, mongoDb, tableName, mongoDbLevel) {
    console.log(`🔄 Đang xóa user MIGRATE cũ...`);
    await deleteOldUsers(mongoDb)

    console.log(`🔄 Đang di chuyển bảng ${tableName}...`);

    const managerLevel1 = await findManagerLevel(mongoDbLevel, 'level-1');
    const managerLevel2 = await findManagerLevel(mongoDbLevel, 'level-2');
    const managerLevel3 = await findManagerLevel(mongoDbLevel, 'level-3');

    let offset = 0;
    while (true) {
        const rows = await fetchBatch(sqlConnection, tableName, offset, BATCH_SIZE);
        const mappedUsers = [];

        for (const row of rows) {

            const managerLevel = await fetchManagerLevel(sqlConnection, row.Id)

            const level1 = managerLevel1.find(item => managerLevel.includes(item.oldId))
            const level2 = managerLevel2.find(item => managerLevel.includes(item.oldId))
            const level3 = managerLevel3.find(item => managerLevel.includes(item.oldId))

            mappedUsers.push({
                accessFailedCount: 0,
                address: "",
                avatar: row.AvatarUrl ? row.AvatarUrl.replace("https://cdn4t.mobiedu.vn", "https://media-moocs.mobifone.vn") : "",
                birthday: row.Birthday,
                dateLogin: row.LastLoginDate,
                email: row.Email || "",
                fullname: row.FullName,
                gender: row.IdGender || 0,
                exploreField: {},
                siteId: +NEW_SITE_ID,
                positionId: -1,
                infoManagementLevel: null,
                infoManagementLevel: {
                    managerLevel1Id: level3?.level1.toString() || level2?.level1.toString() || level1?._id.toString() || "",
                    managerLevel2Id: level3?.level2.toString() || level2?._id.toString() || "",
                    managerLevel3Id: level3?._id.toString() || "",
                },
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
                listRoles: {},
                listPolicy: [],
                role: getRoleName(row.IdType),
                functionsTree: {},
                idTeacher: "",
            });
        }

        // Kiểm tra nếu rows trống thì thoát khỏi vòng lặp
        if (!rows || rows.length === 0) {
            break;
        }

        await mongoDb.collection('user').insertMany(mappedUsers);

        offset += +BATCH_SIZE;
    }

    console.log(`🏁 Hoàn tất di chuyển bảng ${tableName}!`);
}

const sqlConnection = await connectMySQL();

// Chạy quá trình di chuyển
async function migrate() {
    const mongoClient = new MongoClient(MONGO_URL);
    await mongoClient.connect();
    const mongoDb = mongoClient.db(MONGO_DATABASE_USER);
    const mongoDbLevel = mongoClient.db(MONGO_DATABASE_LEVEL);


    try {
        await migrateTable(sqlConnection, mongoDb, 'Users', mongoDbLevel);
    } catch (error) {
        console.error("❌ Lỗi khi di chuyển dữ liệu:", error);
    } finally {
        await sqlConnection.end();
        await mongoClient.close();
        console.log("🔚 Đã hoàn thành quá trình di chuyển.");
    }
}

migrate();
