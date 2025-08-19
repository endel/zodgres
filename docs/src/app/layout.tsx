import '@/app/global.css';
import { RootProvider } from 'fumadocs-ui/provider';
import { Inter } from 'next/font/google';
import Script from 'next/script';
import type { ReactNode } from 'react';
import type { Metadata } from 'next';
import DefaultSearchDialog from '@/components/search';

const inter = Inter({
  subsets: ['latin'],
});

const GA_TRACKING_ID = 'G-SJ8VQE9F8X';

export const metadata: Metadata = {
  title: 'Zodgres Documentation',
  description: 'Zodgres - Type-safe PostgreSQL collections with Zod schemas',
  icons: {
    icon: '/favicon.png',
  },
};

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className={inter.className} suppressHydrationWarning>
      <body className="flex flex-col min-h-screen">
        {/* Google Analytics */}
        <Script
          src={`https://www.googletagmanager.com/gtag/js?id=${GA_TRACKING_ID}`}
          strategy="afterInteractive"
        />
        <Script id="google-analytics" strategy="afterInteractive">
          {`
            window.dataLayer = window.dataLayer || [];
            function gtag(){dataLayer.push(arguments);}
            gtag('js', new Date());
            gtag('config', '${GA_TRACKING_ID}');
          `}
        </Script>
        <RootProvider search={{ SearchDialog: DefaultSearchDialog }}>{children}</RootProvider>
      </body>
    </html>
  );
}
