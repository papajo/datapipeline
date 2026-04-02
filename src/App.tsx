/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import React, { useState, useEffect } from 'react';
import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, 
  BarChart, Bar, Legend, AreaChart, Area 
} from 'recharts';
import { 
  Database, 
  RefreshCw, 
  CloudRain, 
  Thermometer, 
  Droplets, 
  CheckCircle2, 
  AlertCircle,
  ArrowRight,
  Server,
  Layers,
  Upload,
  FileUp,
  MapPin
} from 'lucide-react';
import { collection, query, orderBy, limit, onSnapshot, getDocs, writeBatch, doc, setDoc, getDoc } from 'firebase/firestore';
import { 
  GoogleAuthProvider, 
  signInWithPopup, 
  onAuthStateChanged, 
  signOut 
} from 'firebase/auth';
import { db, auth } from './firebase';
import { format, parseISO } from 'date-fns';
import { motion, AnimatePresence } from 'motion/react';

enum OperationType {
  CREATE = 'create',
  UPDATE = 'update',
  DELETE = 'delete',
  LIST = 'list',
  GET = 'get',
  WRITE = 'write',
}

interface FirestoreErrorInfo {
  error: string;
  operationType: OperationType;
  path: string | null;
  authInfo: {
    userId: string | undefined;
    email: string | null | undefined;
    emailVerified: boolean | undefined;
    isAnonymous: boolean | undefined;
    providerInfo: any[];
  }
}

function handleFirestoreError(error: unknown, operationType: OperationType, path: string | null) {
  const errInfo: FirestoreErrorInfo = {
    error: error instanceof Error ? error.message : String(error),
    authInfo: {
      userId: auth.currentUser?.uid,
      email: auth.currentUser?.email,
      emailVerified: auth.currentUser?.emailVerified,
      isAnonymous: auth.currentUser?.isAnonymous,
      providerInfo: auth.currentUser?.providerData.map(provider => ({
        providerId: provider.providerId,
        displayName: provider.displayName,
        email: provider.email,
        photoUrl: provider.photoURL
      })) || []
    },
    operationType,
    path
  }
  console.error('Firestore Error: ', JSON.stringify(errInfo));
  return new Error(error instanceof Error ? error.message : 'Missing or insufficient permissions');
}

export default function App() {
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [pipelineRunning, setPipelineRunning] = useState(false);
  const [pipelineStep, setPipelineStep] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [status, setStatus] = useState<{ type: 'success' | 'error' | null, message: string }>({ type: null, message: '' });
  const [user, setUser] = useState<any>(null);
  const [authChecking, setAuthChecking] = useState(true);
  const [isAdmin, setIsAdmin] = useState(false);
  const [dataType, setDataType] = useState<'weather' | 'generic'>('weather');
  const [genericData, setGenericData] = useState<any[]>([]);
  const [ingestUrl, setIngestUrl] = useState('');
  const [ingestLocation, setIngestLocation] = useState('');

  useEffect(() => {
    const unsubscribeAuth = onAuthStateChanged(auth, async (user) => {
      if (user) {
        // Ensure user document exists
        const userRef = doc(db, 'users', user.uid);
        try {
          const userSnap = await getDoc(userRef);
          const isAdminEmail = ['pseaguy@gmail.com', 'joshipv2@gmail.com'].includes(user.email?.toLowerCase() || '');
          
          if (!userSnap.exists()) {
            const role = isAdminEmail ? 'admin' : 'user';
            await setDoc(userRef, {
              uid: user.uid,
              email: user.email,
              displayName: user.displayName,
              role: role,
              createdAt: new Date().toISOString()
            });
            setIsAdmin(role === 'admin');
          } else {
            setIsAdmin(userSnap.data()?.role === 'admin' || isAdminEmail);
          }
        } catch (e) {
          console.error("Error creating/checking user profile:", e);
        }
      } else {
        setIsAdmin(false);
      }
      setUser(user);
      setAuthChecking(false);
    });

    return () => unsubscribeAuth();
  }, []);

  const handleLogin = async () => {
    const provider = new GoogleAuthProvider();
    try {
      await signInWithPopup(auth, provider);
    } catch (error: any) {
      console.error("Login Error:", error);
      setStatus({ type: 'error', message: `Login failed: ${error.message}` });
    }
  };

  const handleLogout = () => {
    signOut(auth).catch(console.error);
  };

  const processWeatherData = async (rawData: any, location: string, source: string) => {
    setPipelineStep('Transforming & Loading to Weather Warehouse...');
    
    if (!rawData?.hourly?.time) {
      throw new Error('Invalid weather data structure: missing hourly time data.');
    }

    const hourly = rawData.hourly;
    const batch = writeBatch(db);
    const path = 'weather_warehouse';
    const warehouseRef = collection(db, path);

    const totalReadings = hourly.time.length;
    const startIndex = Math.max(0, totalReadings - 48);

    for (let i = startIndex; i < totalReadings; i++) {
      const docId = `reading_${hourly.time[i].replace(/[:.-]/g, '_')}`;
      const docRef = doc(warehouseRef, docId);
      
      const transformedData = {
        timestamp: hourly.time[i],
        temperature: hourly.temperature_2m?.[i] ?? 0,
        humidity: hourly.relative_humidity_2m?.[i] ?? 0,
        precipitation: hourly.precipitation?.[i] ?? 0,
        location: location,
        source: source,
        processedAt: new Date().toISOString()
      };

      batch.set(docRef, transformedData);
    }

    try {
      await batch.commit();
      setDataType('weather');
    } catch (error) {
      throw handleFirestoreError(error, OperationType.WRITE, path);
    }
  };

  const processGenericData = async (rawData: any, source: string, type: string) => {
    setPipelineStep('Transforming & Loading to Generic Warehouse...');
    const batch = writeBatch(db);
    const path = 'generic_warehouse';
    const warehouseRef = collection(db, path);

    const docId = `data_${Date.now()}`;
    const docRef = doc(warehouseRef, docId);
    
    const transformedData = {
      timestamp: new Date().toISOString(),
      payload: rawData,
      source: source,
      dataType: type,
      processedAt: new Date().toISOString()
    };

    batch.set(docRef, transformedData);

    try {
      await batch.commit();
      setDataType('generic');
    } catch (error) {
      throw handleFirestoreError(error, OperationType.WRITE, path);
    }
  };

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setUploading(true);
    setPipelineRunning(true);
    setPipelineStep('Uploading to Lake...');
    setStatus({ type: null, message: '' });
    
    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await fetch('/api/pipeline/upload', {
        method: 'POST',
        body: formData
      });
      
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Server error (${response.status}): ${text.slice(0, 100)}...`);
      }

      const result = await response.json();
      if (result.success) {
        if (result.dataType === 'weather' && result.data) {
          await processWeatherData(result.data, result.location || 'Uploaded File', `User Upload: ${file.name}`);
          setStatus({ type: 'success', message: `Weather data from "${file.name}" processed and moved to warehouse.` });
        } else if (result.data) {
          await processGenericData(result.data, `User Upload: ${file.name}`, result.dataType);
          setStatus({ type: 'success', message: `Generic data from "${file.name}" processed and moved to warehouse.` });
        } else {
          setStatus({ type: 'success', message: `File "${file.name}" uploaded to Data Lake. (Stored for raw analysis)` });
        }
      } else {
        throw new Error(result.error);
      }
    } catch (error: any) {
      console.error("Upload/Process Error:", error);
      setStatus({ type: 'error', message: `Upload failed: ${error.message}` });
    } finally {
      setUploading(false);
      setPipelineRunning(false);
      setPipelineStep(null);
    }
  };

  useEffect(() => {
    if (!user) {
      setData([]);
      setGenericData([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    
    // Listen to Weather Warehouse
    const weatherPath = 'weather_warehouse';
    const weatherQ = query(collection(db, weatherPath), orderBy('timestamp', 'desc'), limit(24));
    const unsubscribeWeather = onSnapshot(weatherQ, (snapshot) => {
      const readings = snapshot.docs.map(doc => ({
        id: doc.id,
        ...doc.data(),
        formattedTime: format(parseISO(doc.data().timestamp), 'HH:mm')
      })).reverse();
      setData(readings);
      
      // Auto-switch if this is the only data
      if (readings.length > 0 && genericData.length === 0) {
        setDataType('weather');
      }
      
      setLoading(false);
    }, (error) => {
      handleFirestoreError(error, OperationType.GET, weatherPath);
    });

    // Listen to Generic Warehouse
    const genericPath = 'generic_warehouse';
    const genericQ = query(collection(db, genericPath), orderBy('timestamp', 'desc'), limit(10));
    const unsubscribeGeneric = onSnapshot(genericQ, (snapshot) => {
      const items = snapshot.docs.map(doc => ({
        id: doc.id,
        ...doc.data(),
        formattedTime: format(parseISO(doc.data().timestamp), 'MMM d, HH:mm')
      }));
      setGenericData(items);

      // Auto-switch if this is the only data
      if (items.length > 0 && data.length === 0) {
        setDataType('generic');
      }
    }, (error) => {
      handleFirestoreError(error, OperationType.GET, genericPath);
    });

    return () => {
      unsubscribeWeather();
      unsubscribeGeneric();
    };
  }, [user]);

  const clearWarehouse = async () => {
    if (!window.confirm('Are you sure you want to clear both data warehouses? This will remove all processed readings.')) return;
    
    setPipelineRunning(true);
    setPipelineStep('Clearing Warehouses...');
    try {
      const batch = writeBatch(db);
      
      const weatherSnapshot = await getDocs(collection(db, 'weather_warehouse'));
      weatherSnapshot.docs.forEach(doc => batch.delete(doc.ref));
      
      const genericSnapshot = await getDocs(collection(db, 'generic_warehouse'));
      genericSnapshot.docs.forEach(doc => batch.delete(doc.ref));
      
      await batch.commit();
      setStatus({ type: 'success', message: 'Warehouses cleared successfully.' });
      setDataType('weather');
    } catch (error: any) {
      console.error("Clear Error:", error);
      setStatus({ type: 'error', message: `Clear failed: ${error.message}` });
    } finally {
      setPipelineRunning(false);
      setPipelineStep(null);
    }
  };

  const runPipeline = async () => {
    setPipelineRunning(true);
    setPipelineStep('Ingesting & Saving to Lake...');
    setStatus({ type: null, message: '' });
    try {
      // 1 & 2: Ingest and Lake (Backend)
      const response = await fetch('/api/pipeline/ingest', { 
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          url: ingestUrl || undefined,
          location: ingestLocation || undefined
        })
      });
      
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Server error (${response.status}): ${text.slice(0, 100)}...`);
      }

      const result = await response.json();
      
      if (!result.success) throw new Error(result.error);

      // 3: Transform & Warehouse (Frontend)
      if (result.dataType === 'weather') {
        await processWeatherData(result.data, result.location, ingestUrl ? `Custom API: ${ingestUrl}` : 'Open-Meteo API');
      } else {
        await processGenericData(result.data, ingestUrl ? `Custom API: ${ingestUrl}` : 'API Ingest', result.dataType || 'unknown');
      }
      
      setStatus({ type: 'success', message: `Pipeline completed! ${result.dataType} data successfully moved to warehouse.` });
      setIngestUrl('');
      setIngestLocation('');
    } catch (error: any) {
      console.error("Pipeline Error:", error);
      setStatus({ type: 'error', message: `Pipeline failed: ${error.message}` });
    } finally {
      setPipelineRunning(false);
      setPipelineStep(null);
    }
  };

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900 font-sans">
      {/* Header */}
      <header className="bg-white border-b border-slate-200 px-6 py-4 sticky top-0 z-10">
        <div className="max-w-7xl mx-auto flex justify-between items-center">
          <div className="flex items-center gap-3">
            <div className="bg-indigo-600 p-2 rounded-lg">
              <Database className="text-white w-6 h-6" />
            </div>
            <div>
              <h1 className="text-xl font-bold tracking-tight">DataPipeline Dashboard</h1>
              <p className="text-xs text-slate-500 font-mono uppercase tracking-wider">End-to-End ETL Demo</p>
            </div>
          </div>
          <div className="flex items-center gap-4">
            {user ? (
              <div className="flex items-center gap-4">
                <div className="hidden lg:flex items-center gap-2 bg-slate-50 border border-slate-200 rounded-full px-3 py-1">
                  <input 
                    type="text" 
                    placeholder="Custom API URL (optional)" 
                    value={ingestUrl}
                    onChange={(e) => setIngestUrl(e.target.value)}
                    className="text-xs bg-transparent border-none focus:ring-0 w-48"
                  />
                  <div className="w-px h-4 bg-slate-200"></div>
                  <input 
                    type="text" 
                    placeholder="Location Name" 
                    value={ingestLocation}
                    onChange={(e) => setIngestLocation(e.target.value)}
                    className="text-xs bg-transparent border-none focus:ring-0 w-32"
                  />
                </div>
                {isAdmin && (
                  <button 
                    onClick={clearWarehouse}
                    disabled={pipelineRunning}
                    className="text-xs text-rose-600 hover:underline font-medium"
                  >
                    Clear Warehouse
                  </button>
                )}
                <div className="hidden sm:flex flex-col items-end">
                  <span className="text-sm font-medium">{user.displayName || 'User'}</span>
                  <button onClick={handleLogout} className="text-xs text-indigo-600 hover:underline">Sign Out</button>
                </div>
                <button 
                  onClick={runPipeline}
                  disabled={pipelineRunning}
                  className={`flex items-center gap-2 px-4 py-2 rounded-full font-medium transition-all ${
                    pipelineRunning 
                      ? 'bg-slate-100 text-slate-400 cursor-not-allowed' 
                      : 'bg-indigo-600 text-white hover:bg-indigo-700 shadow-sm hover:shadow-md active:scale-95'
                  }`}
                >
                  <RefreshCw className={`w-4 h-4 ${pipelineRunning ? 'animate-spin' : ''}`} />
                  {pipelineRunning ? pipelineStep || 'Running...' : 'Trigger Pipeline'}
                </button>
              </div>
            ) : (
              <button 
                onClick={handleLogin}
                className="bg-indigo-600 text-white px-6 py-2 rounded-full font-medium hover:bg-indigo-700 transition-all shadow-sm"
              >
                Sign In with Google
              </button>
            )}
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-8">
        {!user && !authChecking && (
          <div className="flex flex-col items-center justify-center py-20 text-center">
            <div className="bg-indigo-100 p-6 rounded-full mb-6">
              <Database className="w-12 h-12 text-indigo-600" />
            </div>
            <h2 className="text-3xl font-bold mb-4">Welcome to DataPipeline</h2>
            <p className="text-slate-500 max-w-md mb-8">
              Sign in with your Google account to access the real-time weather data warehouse and trigger the ETL pipeline.
            </p>
            <button 
              onClick={handleLogin}
              className="bg-indigo-600 text-white px-8 py-3 rounded-full font-bold text-lg hover:bg-indigo-700 transition-all shadow-lg hover:shadow-xl active:scale-95"
            >
              Get Started
            </button>
          </div>
        )}

        {(user || authChecking) && (
          <>
            {/* Pipeline Status Banner */}
        <AnimatePresence>
          {status.type && (
            <motion.div 
              initial={{ opacity: 0, y: -20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className={`mb-8 p-4 rounded-xl border flex items-center gap-3 ${
                status.type === 'success' ? 'bg-emerald-50 border-emerald-200 text-emerald-800' : 'bg-rose-50 border-rose-200 text-rose-800'
              }`}
            >
              {status.type === 'success' ? <CheckCircle2 className="w-5 h-5" /> : <AlertCircle className="w-5 h-5" />}
              <p className="font-medium">{status.message}</p>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Pipeline Visualization */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-12">
          {[
            { 
              label: 'Ingest', 
              icon: Server, 
              desc: 'API & File Upload',
              action: (
                <label className="mt-2 cursor-pointer flex items-center gap-1 text-[10px] bg-indigo-100 text-indigo-700 px-2 py-1 rounded-md hover:bg-indigo-200 transition-colors">
                  <FileUp className="w-3 h-3" />
                  <span>Upload File</span>
                  <input type="file" className="hidden" onChange={handleFileUpload} disabled={uploading} />
                </label>
              )
            },
            { label: 'Data Lake', icon: Layers, desc: 'Local JSON Storage' },
            { label: 'Warehouse', icon: Database, desc: 'Firestore NoSQL' },
            { label: 'Dashboard', icon: RefreshCw, desc: 'Real-time React' }
          ].map((step, i) => (
            <div key={step.label} className="relative">
              <div className="bg-white p-6 rounded-2xl border border-slate-200 shadow-sm flex flex-col items-center text-center h-full">
                <div className="bg-indigo-50 p-3 rounded-xl mb-3">
                  <step.icon className="w-6 h-6 text-indigo-600" />
                </div>
                <h3 className="font-bold text-slate-800">{step.label}</h3>
                <p className="text-xs text-slate-500 mt-1">{step.desc}</p>
                {step.action}
              </div>
              {i < 3 && (
                <div className="hidden md:block absolute top-1/2 -right-2 transform -translate-y-1/2 z-10">
                  <ArrowRight className="w-4 h-4 text-slate-300" />
                </div>
              )}
            </div>
          ))}
        </div>

        {/* Dashboard Controls */}
        <div className="flex justify-between items-center mb-6">
          <div className="flex gap-2">
            <button 
              onClick={() => setDataType('weather')}
              className={`px-4 py-2 rounded-full text-sm font-bold transition-all ${dataType === 'weather' ? 'bg-indigo-600 text-white shadow-md' : 'bg-white text-slate-600 border border-slate-200 hover:bg-slate-50'}`}
            >
              Weather View
            </button>
            {genericData.length > 0 && (
              <button 
                onClick={() => setDataType('generic')}
                className={`px-4 py-2 rounded-full text-sm font-bold transition-all ${dataType === 'generic' ? 'bg-indigo-600 text-white shadow-md' : 'bg-white text-slate-600 border border-slate-200 hover:bg-slate-50'}`}
              >
                Generic View ({genericData.length})
              </button>
            )}
          </div>
          {isAdmin && (
            <button 
              onClick={clearWarehouse}
              className="flex items-center gap-2 text-rose-600 hover:text-rose-700 font-bold text-sm"
            >
              <RefreshCw className="w-4 h-4" />
              Clear Warehouses
            </button>
          )}
        </div>

        {/* Dashboard Content */}
        {dataType === 'weather' ? (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
            {/* Tile 1: Temperature Trend */}
            <div className="bg-white p-6 rounded-3xl border border-slate-200 shadow-sm">
              <div className="flex justify-between items-start mb-6">
                <div className="flex items-center gap-2">
                  <Thermometer className="w-5 h-5 text-orange-500" />
                  <div>
                    <h2 className="text-lg font-bold leading-tight">Temperature Trend</h2>
                    {data.length > 0 && (
                      <div className="flex flex-col gap-0.5 mt-1">
                        <div className="flex items-center gap-1 text-[10px] text-slate-400">
                          <MapPin className="w-2.5 h-2.5" />
                          <span>{data[0].location || 'Unknown Location'}</span>
                        </div>
                        <div className="flex items-center gap-1 text-[10px] text-indigo-500 font-medium">
                          <Database className="w-2.5 h-2.5" />
                          <span>Source: {data[0].source || 'Unknown'}</span>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
                <span className="text-xs font-mono text-slate-400">Last 24 Hours</span>
              </div>
              <div className="h-[300px] w-full">
                {loading ? (
                  <div className="h-full w-full flex items-center justify-center bg-slate-50 rounded-xl animate-pulse">
                    <p className="text-slate-400 font-medium">Loading warehouse data...</p>
                  </div>
                ) : data.length > 0 ? (
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={data}>
                      <defs>
                        <linearGradient id="colorTemp" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#f97316" stopOpacity={0.3}/>
                          <stop offset="95%" stopColor="#f97316" stopOpacity={0}/>
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                      <XAxis 
                        dataKey="formattedTime" 
                        axisLine={false} 
                        tickLine={false} 
                        tick={{fill: '#94a3b8', fontSize: 12}}
                      />
                      <YAxis 
                        axisLine={false} 
                        tickLine={false} 
                        tick={{fill: '#94a3b8', fontSize: 12}}
                        unit="°C"
                      />
                      <Tooltip 
                        contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                      />
                      <Area 
                        type="monotone" 
                        dataKey="temperature" 
                        stroke="#f97316" 
                        strokeWidth={3}
                        fillOpacity={1} 
                        fill="url(#colorTemp)" 
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="h-full w-full flex flex-col items-center justify-center bg-slate-50 rounded-xl border-2 border-dashed border-slate-200">
                    <p className="text-slate-400 mb-2">Weather Warehouse is empty</p>
                    <button onClick={runPipeline} className="text-indigo-600 font-bold hover:underline">Run Pipeline</button>
                  </div>
                )}
              </div>
            </div>

            {/* Tile 2: Precipitation & Humidity */}
            <div className="bg-white p-6 rounded-3xl border border-slate-200 shadow-sm">
              <div className="flex justify-between items-start mb-6">
                <div className="flex items-center gap-2">
                  <CloudRain className="w-5 h-5 text-blue-500" />
                  <div>
                    <h2 className="text-lg font-bold leading-tight">Precipitation & Humidity</h2>
                    {data.length > 0 && (
                      <div className="flex flex-col gap-0.5 mt-1">
                        <div className="flex items-center gap-1 text-[10px] text-slate-400">
                          <MapPin className="w-2.5 h-2.5" />
                          <span>{data[0].location || 'Unknown Location'}</span>
                        </div>
                        <div className="flex items-center gap-1 text-[10px] text-indigo-500 font-medium">
                          <Database className="w-2.5 h-2.5" />
                          <span>Source: {data[0].source || 'Unknown'}</span>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
                <span className="text-xs font-mono text-slate-400">Last 24 Hours</span>
              </div>
              <div className="h-[300px] w-full">
                {loading ? (
                  <div className="h-full w-full flex items-center justify-center bg-slate-50 rounded-xl animate-pulse">
                    <p className="text-slate-400 font-medium">Loading warehouse data...</p>
                  </div>
                ) : data.length > 0 ? (
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={data}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                      <XAxis 
                        dataKey="formattedTime" 
                        axisLine={false} 
                        tickLine={false} 
                        tick={{fill: '#94a3b8', fontSize: 12}}
                      />
                      <YAxis 
                        axisLine={false} 
                        tickLine={false} 
                        tick={{fill: '#94a3b8', fontSize: 12}}
                      />
                      <Tooltip 
                        contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)' }}
                      />
                      <Legend iconType="circle" wrapperStyle={{ paddingTop: '20px' }} />
                      <Bar dataKey="precipitation" fill="#3b82f6" radius={[4, 4, 0, 0]} name="Precipitation (mm)" />
                      <Bar dataKey="humidity" fill="#94a3b8" radius={[4, 4, 0, 0]} name="Humidity (%)" />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="h-full w-full flex flex-col items-center justify-center bg-slate-50 rounded-xl border-2 border-dashed border-slate-200">
                    <p className="text-slate-400 mb-2">Weather Warehouse is empty</p>
                    <button onClick={runPipeline} className="text-indigo-600 font-bold hover:underline">Run Pipeline</button>
                  </div>
                )}
              </div>
            </div>
          </div>
        ) : (
          <div className="bg-white p-8 rounded-3xl border border-slate-200 shadow-sm">
            <div className="flex justify-between items-center mb-8">
              <div className="flex items-center gap-3">
                <div className="bg-indigo-100 p-2 rounded-lg">
                  <Database className="w-6 h-6 text-indigo-600" />
                </div>
                <div>
                  <h2 className="text-xl font-bold">Generic Data Warehouse</h2>
                  <p className="text-sm text-slate-500">Displaying raw payloads from dynamic ingestion</p>
                </div>
              </div>
              <button 
                onClick={() => setDataType('weather')}
                className="text-sm text-indigo-600 hover:underline font-medium"
              >
                Switch to Weather View
              </button>
            </div>

            <div className="space-y-4">
              {genericData.length > 0 ? (
                genericData.map((item) => (
                  <div key={item.id} className="border border-slate-100 rounded-2xl p-6 hover:bg-slate-50 transition-colors">
                    <div className="flex justify-between items-start mb-4">
                      <div>
                        <span className="bg-indigo-50 text-indigo-700 text-[10px] font-bold uppercase tracking-wider px-2 py-1 rounded-md">
                          {item.dataType}
                        </span>
                        <h4 className="text-sm font-bold text-slate-800 mt-2">Source: {item.source}</h4>
                      </div>
                      <span className="text-xs text-slate-400 font-mono">{item.formattedTime}</span>
                    </div>
                    <div className="bg-slate-900 rounded-xl p-4 overflow-x-auto">
                      <pre className="text-xs text-indigo-300 font-mono">
                        {JSON.stringify(item.payload, null, 2)}
                      </pre>
                    </div>
                  </div>
                ))
              ) : (
                <div className="py-20 text-center border-2 border-dashed border-slate-200 rounded-3xl">
                  <p className="text-slate-400">Generic Warehouse is empty. Upload non-weather JSON to see it here.</p>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Data Lake Info */}
        <div className="mt-12 bg-indigo-900 rounded-3xl p-8 text-white overflow-hidden relative">
          <div className="relative z-10">
            <h2 className="text-2xl font-bold mb-4">Pipeline Architecture</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
              <div>
                <p className="text-indigo-200 mb-6 leading-relaxed">
                  This application demonstrates a complete data lifecycle. Raw data is ingested from external APIs, 
                  persisted in a local "Data Lake" as raw JSON, transformed into a structured schema, and finally 
                  loaded into a Firestore "Data Warehouse" for real-time visualization.
                </p>
                <div className="flex flex-wrap gap-3">
                  {['TypeScript', 'Express', 'Vite', 'Firestore', 'Recharts', 'Tailwind'].map(tech => (
                    <span key={tech} className="bg-indigo-800/50 border border-indigo-700 px-3 py-1 rounded-full text-xs font-medium">
                      {tech}
                    </span>
                  ))}
                </div>
              </div>
              <div className="bg-indigo-800/30 rounded-2xl p-6 border border-indigo-700/50">
                <h4 className="text-sm font-bold uppercase tracking-wider text-indigo-300 mb-4">Current Lake Storage</h4>
                <div className="font-mono text-xs text-indigo-100 space-y-2">
                  <div className="flex justify-between">
                    <span>Path:</span>
                    <span className="text-indigo-300">/data/lake/</span>
                  </div>
                  <div className="flex justify-between">
                    <span>Format:</span>
                    <span className="text-indigo-300">JSON (Raw)</span>
                  </div>
                  <div className="flex justify-between">
                    <span>Warehouse:</span>
                    <span className="text-indigo-300">Firestore</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div className="absolute -right-20 -bottom-20 w-64 h-64 bg-indigo-500/20 rounded-full blur-3xl"></div>
        </div>
          </>
        )}
      </main>
    </div>
  );
}
