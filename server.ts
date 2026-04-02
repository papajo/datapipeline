import express from 'express';
import { createServer as createViteServer } from 'vite';
import path from 'path';
import fs from 'fs';
import axios from 'axios';
import dotenv from 'dotenv';
import multer from 'multer';
import { initializeApp, cert } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';

dotenv.config();

const firebaseConfig = JSON.parse(fs.readFileSync(path.join(process.cwd(), 'firebase-applet-config.json'), 'utf8'));

// Initialize firebase-admin
const adminApp = initializeApp({
  projectId: firebaseConfig.projectId,
});
const db = getFirestore(adminApp, firebaseConfig.firestoreDatabaseId);

// Multer setup for data lake uploads
const LAKE_DIR = path.join(process.cwd(), 'data', 'lake');
if (!fs.existsSync(LAKE_DIR)) {
  fs.mkdirSync(LAKE_DIR, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, LAKE_DIR);
  },
  filename: (req, file, cb) => {
    cb(null, `upload_${Date.now()}_${file.originalname}`);
  }
});
const upload = multer({ storage });

async function startServer() {
  const app = express();
  const PORT = 3000;

  app.use(express.json());

  // API Routes
  app.get('/api/pipeline/status', (req, res) => {
    res.json({ status: 'ready', warehouse: 'Firestore', lake: LAKE_DIR });
  });

  // New Upload Endpoint
  app.post('/api/pipeline/upload', upload.single('file'), (req: any, res) => {
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'No file uploaded' });
    }
    
    let fileData = null;
    let dataType = 'unknown';

    // Attempt to parse if it's a JSON file
    if (req.file.mimetype === 'application/json' || req.file.originalname.endsWith('.json')) {
      try {
        const content = fs.readFileSync(req.file.path, 'utf8');
        fileData = JSON.parse(content);
        
        // Dynamic Analysis
        if (fileData.hourly && fileData.hourly.time) {
          dataType = 'weather';
        } else if (Array.isArray(fileData)) {
          dataType = 'list';
        } else {
          dataType = 'object';
        }
      } catch (e) {
        console.error("Failed to parse uploaded JSON:", e);
      }
    }

    console.log(`User file uploaded to lake: ${req.file.path} (Type: ${dataType})`);
    res.json({ 
      success: true, 
      message: 'File uploaded to data lake',
      filename: req.file.filename,
      path: req.file.path,
      data: fileData,
      dataType,
      location: dataType === 'weather' ? "Uploaded Weather Data" : "Uploaded Generic Data"
    });
  });

  app.post('/api/pipeline/ingest', async (req, res) => {
    try {
      const { url: customUrl, location: customLocation } = req.body;
      console.log(`Ingesting data from: ${customUrl || 'Default Open-Meteo'}...`);

      // 1. INGEST
      const lat = 51.5074;
      const lon = -0.1278;
      const defaultLocation = "London, UK";
      const defaultUrl = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&hourly=temperature_2m,relative_humidity_2m,precipitation&past_days=7&forecast_days=1`;
      
      const url = customUrl || defaultUrl;
      const location = customLocation || defaultLocation;

      const response = await axios.get(url);
      const rawData = response.data;
      
      // Dynamic Analysis
      let dataType = 'unknown';
      if (rawData.hourly && rawData.hourly.time) {
        dataType = 'weather';
      } else if (Array.isArray(rawData)) {
        dataType = 'list';
      } else {
        dataType = 'object';
      }

      // 2. LAKE (Store raw data)
      const prefix = dataType === 'weather' ? 'weather' : 'generic';
      const lakePath = path.join(LAKE_DIR, `${prefix}_${Date.now()}.json`);
      fs.writeFileSync(lakePath, JSON.stringify(rawData, null, 2));
      console.log(`Raw data stored in lake: ${lakePath} (Type: ${dataType})`);

      // Return data to client for transformation and warehouse loading
      res.json({ 
        success: true, 
        data: rawData,
        dataType,
        location,
        lakePath
      });
    } catch (error: any) {
      console.error('Ingestion failed:', error);
      res.status(500).json({ success: false, error: error.message });
    }
  });

  // Vite middleware for development
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });

  // Global Error Handler
  app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
    console.error('Server Error:', err);
    res.status(500).json({ success: false, error: err.message || 'Internal Server Error' });
  });
}

startServer();
