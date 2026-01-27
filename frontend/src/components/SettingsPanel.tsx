import { useRef, useEffect, useCallback } from 'react';

interface Node {
  x: number;
  y: number;
  baseX: number;
  baseY: number;
  vx: number;
  vy: number;
}

interface Helix {
  nodes: Node[][];
  cx: number;
  cy: number;
  angle: number;
  length: number;
  amplitude: number;
  speed: number;
}

interface SettingsPanelProps {
  isOpen: boolean;
  onClose: () => void;
}

export function SettingsPanel({ isOpen, onClose }: SettingsPanelProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const mouseRef = useRef<{ x: number; y: number }>({ x: -9999, y: -9999 });
  const helicesRef = useRef<Helix[]>([]);
  const animRef = useRef<number>(0);
  const timeRef = useRef<number>(0);
  const sizeRef = useRef<{ w: number; h: number }>({ w: 0, h: 0 });

  const NODES_PER_STRAND = 40;
  const MOUSE_RADIUS = 150;
  const SPRING = 0.018;
  const DAMPING = 0.95;
  const BREAK_FORCE = 18;

  const createHelix = useCallback((cx: number, cy: number, length: number, amplitude: number, angle: number, speed: number): Helix => {
    const strands: Node[][] = [[], []];
    const spacing = length / (NODES_PER_STRAND - 1);

    for (let i = 0; i < NODES_PER_STRAND; i++) {
      const t = i * spacing - length / 2;
      const phase1 = (i / NODES_PER_STRAND) * Math.PI * 6;
      const phase2 = phase1 + Math.PI;

      const cosA = Math.cos(angle);
      const sinA = Math.sin(angle);

      // Position along the helix axis
      const axisX = cx + t * cosA;
      const axisY = cy + t * sinA;

      // Perpendicular offset for the helix
      const perpX = -sinA;
      const perpY = cosA;

      const offset1 = Math.sin(phase1) * amplitude;
      const offset2 = Math.sin(phase2) * amplitude;

      strands[0].push({
        x: axisX + perpX * offset1,
        y: axisY + perpY * offset1,
        baseX: axisX + perpX * offset1,
        baseY: axisY + perpY * offset1,
        vx: 0, vy: 0,
      });
      strands[1].push({
        x: axisX + perpX * offset2,
        y: axisY + perpY * offset2,
        baseX: axisX + perpX * offset2,
        baseY: axisY + perpY * offset2,
        vx: 0, vy: 0,
      });
    }

    return { nodes: strands, cx, cy, angle, length, amplitude, speed };
  }, []);

  const initHelices = useCallback(() => {
    const { w, h } = sizeRef.current;
    if (w === 0 || h === 0) return;

    const helices: Helix[] = [];
    // Spread multiple helices across the screen
    const count = Math.max(3, Math.floor((w * h) / 180000));

    for (let i = 0; i < count; i++) {
      const cx = (w * (i + 1)) / (count + 1) + (Math.random() - 0.5) * w * 0.15;
      const cy = h * (0.3 + Math.random() * 0.4);
      const length = h * (0.5 + Math.random() * 0.35);
      const amplitude = 30 + Math.random() * 50;
      const angle = -Math.PI / 2 + (Math.random() - 0.5) * 0.4;
      const speed = 0.003 + Math.random() * 0.004;
      helices.push(createHelix(cx, cy, length, amplitude, angle, speed));
    }

    helicesRef.current = helices;
  }, [createHelix]);

  // Handle resize
  useEffect(() => {
    if (!isOpen) return;

    const resize = () => {
      const w = window.innerWidth;
      const h = window.innerHeight;
      sizeRef.current = { w, h };
      if (canvasRef.current) {
        canvasRef.current.width = w;
        canvasRef.current.height = h;
      }
      initHelices();
    };

    resize();
    window.addEventListener('resize', resize);
    return () => window.removeEventListener('resize', resize);
  }, [isOpen, initHelices]);

  // Animation loop
  useEffect(() => {
    if (!isOpen) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const animate = () => {
      timeRef.current += 1;
      const { w, h } = sizeRef.current;
      ctx.clearRect(0, 0, w, h);

      const mouse = mouseRef.current;

      for (const helix of helicesRef.current) {
        const { nodes, cx, cy, angle, length, amplitude, speed } = helix;
        const cosA = Math.cos(angle);
        const sinA = Math.sin(angle);
        const perpX = -sinA;
        const perpY = cosA;

        // Update base positions (rotation) and physics
        for (let s = 0; s < 2; s++) {
          for (let i = 0; i < nodes[s].length; i++) {
            const node = nodes[s][i];
            const t = (i / (NODES_PER_STRAND - 1)) * length - length / 2;
            const phase = ((i / NODES_PER_STRAND) * Math.PI * 6) + timeRef.current * speed + (s * Math.PI);
            const offset = Math.sin(phase) * amplitude;

            const axisX = cx + t * cosA;
            const axisY = cy + t * sinA;
            node.baseX = axisX + perpX * offset;
            node.baseY = axisY + perpY * offset;

            // Mouse repulsion
            const dx = node.x - mouse.x;
            const dy = node.y - mouse.y;
            const dist = Math.sqrt(dx * dx + dy * dy);
            if (dist < MOUSE_RADIUS && dist > 0) {
              const force = ((1 - dist / MOUSE_RADIUS) ** 2) * BREAK_FORCE;
              node.vx += (dx / dist) * force;
              node.vy += (dy / dist) * force;
            }

            // Spring back
            node.vx += (node.baseX - node.x) * SPRING;
            node.vy += (node.baseY - node.y) * SPRING;
            node.vx *= DAMPING;
            node.vy *= DAMPING;
            node.x += node.vx;
            node.y += node.vy;
          }
        }

        // Draw rungs
        for (let i = 0; i < NODES_PER_STRAND; i++) {
          const a = nodes[0][i];
          const b = nodes[1][i];
          const dx = a.x - b.x;
          const dy = a.y - b.y;
          const dist = Math.sqrt(dx * dx + dy * dy);
          const baseDx = a.baseX - b.baseX;
          const baseDy = a.baseY - b.baseY;
          const baseDist = Math.sqrt(baseDx * baseDx + baseDy * baseDy) || 1;
          const stretch = dist / baseDist;

          if (stretch < 2.5) {
            const opacity = Math.max(0, 0.35 * (1 - (stretch - 1) * 0.7));
            ctx.beginPath();
            ctx.moveTo(a.x, a.y);
            ctx.lineTo(b.x, b.y);
            ctx.strokeStyle = `rgba(255, 255, 255, ${opacity})`;
            ctx.lineWidth = 0.5;
            ctx.stroke();
          }
        }

        // Draw strands
        for (let s = 0; s < 2; s++) {
          const strand = nodes[s];
          ctx.beginPath();
          ctx.moveTo(strand[0].x, strand[0].y);
          for (let i = 1; i < strand.length; i++) {
            const prev = strand[i - 1];
            const curr = strand[i];
            const cpx = (prev.x + curr.x) / 2;
            const cpy = (prev.y + curr.y) / 2;
            ctx.quadraticCurveTo(prev.x, prev.y, cpx, cpy);
          }
          ctx.strokeStyle = 'rgba(255, 255, 255, 0.7)';
          ctx.lineWidth = 1.2;
          ctx.stroke();

          // Nodes
          for (let i = 0; i < strand.length; i++) {
            const node = strand[i];
            const ddx = node.x - node.baseX;
            const ddy = node.y - node.baseY;
            const displacement = Math.sqrt(ddx * ddx + ddy * ddy);
            const intensity = Math.min(displacement / 60, 1);
            const radius = 1.5 + intensity * 2;
            const alpha = 0.4 + intensity * 0.6;
            ctx.beginPath();
            ctx.arc(node.x, node.y, radius, 0, Math.PI * 2);
            ctx.fillStyle = `rgba(255, 255, 255, ${alpha})`;
            ctx.fill();
          }
        }
      }

      animRef.current = requestAnimationFrame(animate);
    };

    animRef.current = requestAnimationFrame(animate);
    return () => cancelAnimationFrame(animRef.current);
  }, [isOpen]);

  // ESC to close
  useEffect(() => {
    if (!isOpen) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 z-50 bg-black cursor-crosshair"
      onMouseMove={(e) => {
        mouseRef.current = { x: e.clientX, y: e.clientY };
      }}
      onMouseLeave={() => {
        mouseRef.current = { x: -9999, y: -9999 };
      }}
      onClick={onClose}
    >
      <canvas ref={canvasRef} className="w-full h-full" />
    </div>
  );
}

export default SettingsPanel;
