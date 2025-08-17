import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

/**
 * Shared layout configurations
 *
 * you can customise layouts individually from:
 * Home Layout: app/(home)/layout.tsx
 * Docs Layout: app/docs/layout.tsx
 */
export const baseOptions: BaseLayoutProps = {
  nav: {
    title: (
      <>
        <img
          src="/favicon.png"
          alt="Zodgres Logo"
          width="24"
          height="24"
          className="inline-block"
        />
        Zodgres
      </>
    ),
  },
  githubUrl: 'https://github.com/endel/zodgres',
};
