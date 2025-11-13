'use client';

import { useTheme } from 'next-themes';
import { useEffect, useState } from 'react';
import { ThemeToggleButton, ThemeToggleButtonProps, useThemeTransition } from './theme-toggle-button';

type ThemeToggleProps = Omit<ThemeToggleButtonProps, 'theme' | 'onClick'>;

export function ThemeToggle(props: ThemeToggleProps) {
  const { theme, setTheme } = useTheme();
  const { startTransition } = useThemeTransition();
  const [mounted, setMounted] = useState(false);

  // Avoid hydration mismatch
  useEffect(() => {
    setMounted(true);
  }, []);

  if (!mounted) {
    return null;
  }

  const handleToggle = () => {
    startTransition(() => {
      setTheme(theme === 'light' ? 'dark' : 'light');
    });
  };

  return (
    <ThemeToggleButton
      {...props}
      theme={theme as 'light' | 'dark'}
      onClick={handleToggle}
    />
  );
}
