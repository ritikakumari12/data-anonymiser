const express = require('express');
const router = express.Router();
const AWS=require('aws-sdk');
const crypto=require('crypto');
const csv=require('csv-parser');
const {Parser} = require('json2csv');
const stream=require('stream');

const algorithm = 'aes-256-ctr';
const secretKey = process.env.CRYPT_SECRET;

/**
 * @swagger
 * /api/crypt:
 *   post:
 *     summary: Encrypt or Decrypt selected columns of a CSV file
 *     tags: [Crypt]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required:
 *               - key
 *               - operation
 *               - columns
 *             properties:
 *               key:
 *                 type: string
 *                 example: "uploads/customers.csv"
 *                 description: The S3 key (filename) of the CSV file
 *               operation:
 *                 type: string
 *                 enum: [encrypt, decrypt]
 *                 example: encrypt
 *                 description: Whether to encrypt or decrypt
 *               columns:
 *                 type: array
 *                 items:
 *                   type: string
 *                 example: ["First Name", "Email"]
 *                 description: List of column names to process
 *     responses:
 *       200:
 *         description: File successfully processed
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 message:
 *                   type: string
 *                   example: "encrypt successful"
 *                 key:
 *                   type: string
 *                   example: "processed/1714403212_encrypt_customers.csv"
 *       400:
 *         description: Bad request (missing or invalid input)
 *       500:
 *         description: Server error (file processing failed)
 */


function encrypt(text) {
    const cipher = crypto.createCipher(algorithm, secretKey);
    return cipher.update(text, 'utf8', 'hex') + cipher.final('hex');
  }

function decrypt(text) {
    const decipher = crypto.createDecipher(algorithm, secretKey);
    return decipher.update(text, 'hex', 'utf8') + decipher.final('utf8');
}

router.post('/', async (req, res) => {
    let { key, operation, columns } = req.body; 
  if (!key && req.body.fileKey) key = req.body.fileKey;

  if (typeof columns === 'string') {
    columns = columns.split(',').map(s => s.trim()).filter(Boolean);
  }

  if (!key || !operation || !Array.isArray(columns) || columns.length === 0) {
    return res.status(400).json({ error: 'Missing or invalid key, operation, or columns' });
  }
  
    const s3 = new AWS.S3({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.AWS_REGION,
    });

    try {
        const params = { Bucket: process.env.AWS_S3_BUCKET, Key: key };
        const s3Stream = s3.getObject(params).createReadStream();
    
        const results = [];
        const transform = (row) => {
          for (const col of columns) {
            if (row[col]) {
              row[col] = operation === 'encrypt' ? encrypt(row[col]) : decrypt(row[col]);
            }
          }
          return row;
        };
    await new Promise((resolve, reject) => {
        s3Stream
          .pipe(csv())
          .on('data', (row) => results.push(transform(row)))
          .on('end', resolve)
          .on('error', reject);
      });
  
      const parser = new Parser();
      const csvData = parser.parse(results);
  
      const newKey = `processed/${Date.now()}_${operation}_${key.split('/').pop()}`;
      await s3.upload({
        Bucket: process.env.AWS_S3_BUCKET,
        Key: newKey,
        Body: csvData,
        ContentType: 'text/csv'
      }).promise();
  
      res.json({ message: `${operation} successful`, key: newKey });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: 'Failed to process file' });
    }
  });
  
  module.exports = router;

